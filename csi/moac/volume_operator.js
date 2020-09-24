"use strict";
// Volume operator managing volume k8s custom resources.
//
// Primary motivation for the resource is to provide information about
// existing volumes. Other actions and their consequences follow:
//
// * destroying the resource implies volume destruction (not advisable)
// * creating the resource implies volume import (not advisable)
// * modification of "preferred nodes" property influences scheduling of new replicas
// * modification of "required nodes" property moves the volume to different nodes
// * modification of replica count property changes redundancy of the volume
//
// Volume operator stands between k8s custom resource (CR) describing desired
// state and volume manager reflecting the actual state. It gets new/mod/del
// events from both, from the world of ideas and from the world of material
// things. It's task which is not easy, is to restore harmony between them:
//
// +---------+ new/mod/del  +----------+  new/mod/del  +-----------+
// | Volumes +--------------> Operator <---------------+  Watcher  |
// +------^--+              ++--------++               +---^-------+
//        |                  |        |                    |
//        |                  |        |                    |
//        +------------------+        +--------------------+
//       create/modify/destroy         create/modify/destroy
//
//
//  real object event  |    CR exists    |  CR does not exist
// ------------------------------------------------------------
//        new          |      --         |   create CR
//        mod          |    modify CR    |      --
//        del          |    delete CR    |      --
//
//
//      CR event       |  volume exists  |  volume does not exist
// ---------------------------------------------------------------
//        new          |  modify volume  |   create volume
//        mod          |  modify volume  |      --
//        del          |  delete volume  |      --
//
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const yaml = require('js-yaml');
const EventStream = require('./event_stream');
const log = require('./logger').Logger('volume-operator');
const Workq = require('./workq');
const assert_1 = __importDefault(require("assert"));
const fs = __importStar(require("fs"));
const _ = __importStar(require("lodash"));
const path = __importStar(require("path"));
const client_node_1 = require("@kubernetes/client-node");
const cache_1 = require("./cache");
const RESOURCE_NAME = 'mayastorvolume';
const crdVolume = yaml.safeLoad(fs.readFileSync(path.join(__dirname, '/crds/mayastorvolume.yaml'), 'utf8'));
// lower-case letters uuid pattern
const uuidRegexp = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$/;
// Protocol used to export nexus (volume)
var Protocol;
(function (Protocol) {
    Protocol["Unknown"] = "unknown";
    Protocol["Nbd"] = "nbd";
    Protocol["Iscsi"] = "iscsi";
    Protocol["Nvmf"] = "nvmf";
})(Protocol || (Protocol = {}));
function protocolFromString(val) {
    if (val == Protocol.Nbd) {
        return Protocol.Nbd;
    }
    else if (val == Protocol.Iscsi) {
        return Protocol.Iscsi;
    }
    else if (val == Protocol.Nvmf) {
        return Protocol.Nvmf;
    }
    else {
        return Protocol.Unknown;
    }
}
// State of the volume
var State;
(function (State) {
    State["Unknown"] = "unknown";
    State["Online"] = "online";
    State["Offline"] = "offline";
    State["Degraded"] = "degraded";
    State["Faulted"] = "faulted";
    State["Failed"] = "failed";
})(State || (State = {}));
function stateFromString(val) {
    if (val == State.Online) {
        return State.Online;
    }
    else if (val == State.Offline) {
        return State.Offline;
    }
    else if (val == State.Degraded) {
        return State.Degraded;
    }
    else if (val == State.Faulted) {
        return State.Faulted;
    }
    else if (val == State.Failed) {
        return State.Failed;
    }
    else {
        return State.Unknown;
    }
}
// Object defines properties of node resource.
class VolumeResource extends cache_1.CustomResource {
    constructor(cr) {
        var _a;
        super();
        this.apiVersion = cr.apiVersion;
        this.kind = cr.kind;
        if (((_a = cr.metadata) === null || _a === void 0 ? void 0 : _a.name) === undefined) {
            throw new Error('Missing name attribute');
        }
        this.metadata = cr.metadata;
        if (!cr.metadata.name.match(uuidRegexp)) {
            throw new Error(`Invalid UUID`);
        }
        let spec = cr.spec;
        if (spec === undefined) {
            throw new Error('Missing spec section');
        }
        if (!spec.requiredBytes) {
            throw new Error('Missing requiredBytes');
        }
        this.spec = {
            replicaCount: spec.replicaCount || 1,
            preferredNodes: [].concat(spec.preferredNodes || []).sort(),
            requiredNodes: [].concat(spec.requiredNodes || []).sort(),
            requiredBytes: spec.requiredBytes,
            limitBytes: spec.limitBytes || 0,
            protocol: protocolFromString(spec.protocol)
        };
        let status = cr.status;
        if (status !== undefined) {
            this.status = {
                size: status.size || 0,
                state: stateFromString(status.state),
                node: status.node,
                // sort the replicas according to uri to have deterministic order
                replicas: [].concat(status.replicas || []).sort((a, b) => {
                    if (a.uri < b.uri)
                        return -1;
                    else if (a.uri > b.uri)
                        return 1;
                    else
                        return 0;
                }),
                nexus: status.nexus
            };
        }
    }
    getUuid() {
        let uuid = this.metadata.name;
        if (uuid === undefined) {
            throw new Error('Volume resource without UUID');
        }
        else {
            return uuid;
        }
    }
}
// Volume operator managing volume k8s custom resources.
class VolumeOperator {
    // concurrent changes to volumes.
    // Create volume operator object.
    //
    // @param namespace   Namespace the operator should operate on.
    // @param kubeConfig  KubeConfig.
    // @param volumes     Volume manager.
    constructor(namespace, kubeConfig, volumes) {
        this.namespace = namespace;
        this.volumes = volumes;
        this.eventStream = null;
        this.workq = new Workq();
        this.watcher = new cache_1.CustomResourceCache(this.namespace, RESOURCE_NAME, kubeConfig, VolumeResource);
    }
    // Create volume CRD if it doesn't exist.
    //
    // @param kubeConfig  KubeConfig.
    async init(kubeConfig) {
        log.info('Initializing volume operator');
        let k8sExtApi = kubeConfig.makeApiClient(client_node_1.ApiextensionsV1Api);
        try {
            await k8sExtApi.createCustomResourceDefinition(crdVolume);
            log.info(`Created CRD ${RESOURCE_NAME}`);
        }
        catch (err) {
            // API returns a 409 Conflict if CRD already exists.
            if (err.statusCode !== 409)
                throw err;
        }
    }
    // Start volume operator's watcher loop.
    //
    // NOTE: Not getting the start sequence right can have catastrophic
    // consequence leading to unintended volume destruction and data loss.
    //
    async start() {
        var self = this;
        // install event handlers to follow changes to resources.
        this._bindWatcher(this.watcher);
        await this.watcher.start();
        // This will start async processing of volume events.
        this.eventStream = new EventStream({ volumes: this.volumes });
        this.eventStream.on('data', async (ev) => {
            // the only kind of event that comes from the volumes source
            assert_1.default(ev.kind === 'volume');
            self.workq.push(ev, self._onVolumeEvent.bind(self));
        });
    }
    async _onVolumeEvent(ev) {
        const uuid = ev.object.uuid;
        if (ev.eventType === 'new' || ev.eventType === 'mod') {
            const origObj = this.watcher.get(name);
            const spec = {
                replicaCount: ev.object.replicaCount,
                preferredNodes: _.clone(ev.object.preferredNodes),
                requiredNodes: _.clone(ev.object.requiredNodes),
                requiredBytes: ev.object.requiredBytes,
                limitBytes: ev.object.limitBytes,
                protocol: protocolFromString(ev.object.protocol)
            };
            const status = this._volumeToStatus(ev.object);
            if (origObj !== undefined) {
                await this._updateSpec(uuid, origObj, spec);
            }
            else if (ev.eventType === 'new') {
                await this._createResource(uuid, spec);
            }
            await this._updateStatus(uuid, status);
        }
        else if (ev.eventType === 'del') {
            await this._deleteResource(uuid);
        }
        else {
            assert_1.default(false);
        }
    }
    // Transform volume to status properties used in k8s volume resource.
    //
    // @param   volume   Volume object.
    // @returns Status properties.
    //
    _volumeToStatus(volume) {
        const st = {
            size: volume.getSize(),
            state: stateFromString(volume.state),
            node: volume.getNodeName(),
            replicas: Object.values(volume.replicas).map((r) => {
                return {
                    node: r.pool.node.name,
                    pool: r.pool.name,
                    uri: r.uri,
                    offline: r.isOffline()
                };
            })
        };
        if (volume.nexus) {
            st.nexus = {
                deviceUri: volume.nexus.deviceUri || '',
                state: volume.nexus.state,
                children: volume.nexus.children.map((ch) => {
                    return {
                        uri: ch.uri,
                        state: ch.state
                    };
                })
            };
        }
        return st;
    }
    // Create k8s CRD object.
    //
    // @param uuid       ID of the created volume.
    // @param spec       New volume spec.
    //
    async _createResource(uuid, spec) {
        try {
            await this.watcher.create({
                apiVersion: 'openebs.io/v1alpha1',
                kind: 'MayastorVolume',
                metadata: {
                    name: uuid,
                    namespace: this.namespace
                },
                spec
            });
        }
        catch (err) {
            log.error(`Failed to create volume resource "${uuid}": ${err}`);
            return;
        }
    }
    // Update properties of k8s CRD object or create it if it does not exist.
    //
    // @param uuid       ID of the updated volume.
    // @param origObj    Existing k8s resource object.
    // @param spec       New volume spec.
    //
    async _updateSpec(uuid, origObj, spec) {
        try {
            await this.watcher.update(name, (orig) => {
                // Update object only if it has really changed
                if (_.isEqual(origObj.spec, spec)) {
                    return;
                }
                log.info(`Updating spec of volume resource "${uuid}"`);
                return {
                    apiVersion: 'openebs.io/v1alpha1',
                    kind: 'MayastorVolume',
                    metadata: orig.metadata,
                    spec,
                };
            });
        }
        catch (err) {
            log.error(`Failed to update volume resource "${uuid}": ${err}`);
            return;
        }
    }
    // Update status of the volume based on real data obtained from storage node.
    //
    // @param uuid    UUID of the resource.
    // @param status  Status properties.
    //
    async _updateStatus(uuid, status) {
        try {
            await this.watcher.updateStatus(name, (orig) => {
                if (_.isEqual(orig.status, status)) {
                    // avoid unnecessary status updates
                    return;
                }
                log.debug(`Updating status of volume resource "${uuid}"`);
                // merge old and new properties
                return {
                    apiVersion: 'openebs.io/v1alpha1',
                    kind: 'MayastorNode',
                    metadata: orig.metadata,
                    spec: orig.spec,
                    status,
                };
            });
        }
        catch (err) {
            log.error(`Failed to update status of volume resource "${uuid}": ${err}`);
        }
    }
    // Set state and reason not touching the other status fields.
    async _updateState(uuid, state, reason) {
        try {
            await this.watcher.updateStatus(name, (orig) => {
                var _a, _b;
                if (((_a = orig.status) === null || _a === void 0 ? void 0 : _a.state) === state && ((_b = orig.status) === null || _b === void 0 ? void 0 : _b.reason) === reason) {
                    // avoid unnecessary status updates
                    return;
                }
                log.debug(`Updating state of volume resource "${uuid}"`);
                // merge old and new properties
                let newStatus = _.assign({}, orig.status, { state, reason });
                return {
                    apiVersion: 'openebs.io/v1alpha1',
                    kind: 'MayastorNode',
                    metadata: orig.metadata,
                    spec: orig.spec,
                    status: newStatus,
                };
            });
        }
        catch (err) {
            log.error(`Failed to update status of volume resource "${uuid}": ${err}`);
        }
    }
    // Delete volume resource with specified uuid.
    //
    // @param uuid   UUID of the volume resource to delete.
    //
    async _deleteResource(uuid) {
        try {
            log.info(`Deleting volume resource "${uuid}"`);
            await this.watcher.delete(uuid);
        }
        catch (err) {
            log.error(`Failed to delete volume resource "${uuid}": ${err}`);
        }
    }
    // Stop listening for watcher and node events and reset the cache
    async stop() {
        this.watcher.removeAllListeners();
        if (this.eventStream) {
            this.eventStream.destroy();
            this.eventStream = null;
        }
    }
    // Bind watcher's new/mod/del events to volume operator's callbacks.
    //
    // @param watcher   k8s volume resource cache.
    //
    _bindWatcher(watcher) {
        var self = this;
        watcher.on('new', (obj) => {
            self.workq.push(obj, self._importVolume.bind(self));
        });
        watcher.on('mod', (obj) => {
            self.workq.push(obj, self._modifyVolume.bind(self));
        });
        watcher.on('del', (obj) => {
            self.workq.push(obj.metadata.name, self._destroyVolume.bind(self));
        });
    }
    // When moac restarts the volume manager does not know which volumes exist.
    // We need to import volumes based on the k8s resources.
    //
    // @param resource    Volume resource properties.
    //
    async _importVolume(resource) {
        const uuid = resource.getUuid();
        log.debug(`Importing volume "${uuid}" in response to "new" resource event`);
        try {
            await this.volumes.importVolume(uuid, resource.spec, resource.status);
        }
        catch (err) {
            log.error(`Failed to import volume "${uuid}" based on new resource: ${err}`);
            await this._updateState(uuid, State.Failed, err.toString());
        }
    }
    // Modify volume according to the specification.
    //
    // @param resource    Volume resource properties.
    //
    async _modifyVolume(resource) {
        const uuid = resource.getUuid();
        const volume = this.volumes.get(uuid);
        if (!volume) {
            log.warn(`Volume resource "${uuid}" was modified but the volume does not exist`);
            return;
        }
        try {
            if (volume.update(resource.spec)) {
                log.debug(`Updating volume "${uuid}" in response to "mod" resource event`);
                volume.fsa();
            }
        }
        catch (err) {
            log.error(`Failed to update volume "${uuid}" based on resource: ${err}`);
        }
    }
    // Remove the volume from internal state and if it exists destroy it.
    //
    // @param uuid   ID of the volume to destroy.
    //
    async _destroyVolume(uuid) {
        log.debug(`Destroying volume "${uuid}" in response to "del" resource event`);
        try {
            await this.volumes.destroyVolume(uuid);
        }
        catch (err) {
            log.error(`Failed to destroy volume "${uuid}": ${err}`);
        }
    }
}
module.exports = VolumeOperator;
