"use strict";
// Node operator is responsible for managing mayastor node custom resources
// that represent nodes in the cluster that run mayastor (storage nodes).
//
// Roles:
// * The operator creates/modifies/deletes the resources to keep them up to date.
// * A user can delete a stale resource (can happen that moac doesn't know)
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
const assert_1 = __importDefault(require("assert"));
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const client_node_1 = require("@kubernetes/client-node");
const cache_1 = require("./cache");
const yaml = require('js-yaml');
const EventStream = require('./event_stream');
const log = require('./logger').Logger('node-operator');
const Workq = require('./workq');
const RESOURCE_NAME = 'mayastornode';
const crdNode = yaml.safeLoad(fs.readFileSync(path.join(__dirname, '/crds/mayastornode.yaml'), 'utf8'));
// State of a storage node.
var NodeState;
(function (NodeState) {
    NodeState["Unknown"] = "unknown";
    NodeState["Online"] = "online";
    NodeState["Offline"] = "offline";
})(NodeState || (NodeState = {}));
// Object defines properties of node resource.
class NodeResource extends cache_1.CustomResource {
    constructor(cr) {
        super();
        this.apiVersion = cr.apiVersion;
        this.kind = cr.kind;
        if (cr.status === NodeState.Online) {
            this.status = NodeState.Online;
        }
        else if (cr.status === NodeState.Offline) {
            this.status = NodeState.Offline;
        }
        else {
            this.status = NodeState.Unknown;
        }
        if (cr.metadata === undefined) {
            throw new Error('missing metadata');
        }
        else {
            this.metadata = cr.metadata;
        }
        if (cr.spec === undefined) {
            throw new Error('missing spec');
        }
        else {
            let grpcEndpoint = cr.spec.grpcEndpoint;
            if (grpcEndpoint === undefined) {
                throw new Error('missing grpc endpoint in spec');
            }
            this.spec = { grpcEndpoint };
        }
    }
}
class NodeOperator {
    // init() is decoupled from constructor because tests do their own
    // initialization of the object.
    //
    // @param namespace   Namespace the operator should operate on.
    // @param kubeConfig  KubeConfig.
    // @param registry    Registry with node objects.
    constructor(namespace, kubeConfig, registry) {
        assert_1.default(registry);
        this.namespace = namespace;
        this.workq = new Workq();
        this.registry = registry;
        this.watcher = new cache_1.CustomResourceCache(this.namespace, RESOURCE_NAME, kubeConfig, NodeResource);
    }
    // Create node CRD if it doesn't exist.
    //
    // @param kubeConfig  KubeConfig.
    async init(kubeConfig) {
        log.info('Initializing node operator');
        let k8sExtApi = kubeConfig.makeApiClient(client_node_1.ApiextensionsV1Api);
        try {
            await k8sExtApi.createCustomResourceDefinition(crdNode);
            log.info(`Created CRD ${RESOURCE_NAME}`);
        }
        catch (err) {
            // API returns a 409 Conflict if CRD already exists.
            if (err.statusCode !== 409)
                throw err;
        }
    }
    // Bind watcher's new/del events to node operator's callbacks.
    //
    // Not interested in mod events as the operator is the only who should
    // be doing modifications to these objects.
    //
    // @param {object} watcher   k8s node resource watcher.
    //
    _bindWatcher(watcher) {
        var self = this;
        watcher.on('new', (obj) => {
            if (obj.metadata)
                self.registry.addNode(obj.metadata.name, obj.spec.grpcEndpoint);
        });
        watcher.on('del', (obj) => {
            self.registry.removeNode(obj.metadata.name);
        });
    }
    // Start node operator's watcher loop.
    async start() {
        var self = this;
        // install event handlers to follow changes to resources.
        self._bindWatcher(self.watcher);
        await self.watcher.start();
        // This will start async processing of node events.
        self.eventStream = new EventStream({ registry: self.registry });
        self.eventStream.on('data', async (ev) => {
            if (ev.kind !== 'node')
                return;
            await self.workq.push(ev, self._onNodeEvent.bind(self));
        });
    }
    async _onNodeEvent(ev) {
        var self = this;
        const name = ev.object.name;
        if (ev.eventType === 'new' || ev.eventType === 'mod') {
            const grpcEndpoint = ev.object.endpoint;
            let origObj = this.watcher.get(name);
            if (origObj === undefined) {
                await this._createResource(name, grpcEndpoint);
            }
            else {
                await this._updateSpec(name, grpcEndpoint);
            }
            await this._updateStatus(name, ev.object.isSynced() ? NodeState.Online : NodeState.Offline);
        }
        else if (ev.eventType === 'del') {
            await self._deleteResource(ev.object.name);
        }
        else {
            assert_1.default.strictEqual(ev.eventType, 'sync');
        }
    }
    async _createResource(name, grpcEndpoint) {
        log.info(`Creating node resource "${name}"`);
        try {
            await this.watcher.create({
                apiVersion: 'openebs.io/v1alpha1',
                kind: 'MayastorNode',
                metadata: {
                    name,
                    namespace: this.namespace
                },
                spec: { grpcEndpoint }
            });
        }
        catch (err) {
            log.error(`Failed to create node resource "${name}": ${err}`);
        }
    }
    // Update properties of k8s CRD object or create it if it does not exist.
    //
    // @param name          Name of the updated node.
    // @param grpcEndpoint  Endpoint property of the object.
    //
    async _updateSpec(name, grpcEndpoint) {
        try {
            await this.watcher.update(name, (orig) => {
                // Update object only if it has really changed
                if (orig.spec.grpcEndpoint === grpcEndpoint) {
                    return;
                }
                log.info(`Updating spec of node resource "${name}"`);
                return {
                    apiVersion: 'openebs.io/v1alpha1',
                    kind: 'MayastorNode',
                    metadata: orig.metadata,
                    spec: { grpcEndpoint }
                };
            });
        }
        catch (err) {
            log.error(`Failed to update node resource "${name}": ${err}`);
        }
    }
    // Update state of the resource.
    //
    // NOTE: This method does not throw if the operation fails as there is nothing
    // we can do if it fails. Though we log an error message in such a case.
    //
    // @param name    UUID of the resource.
    // @param status  State of the node.
    //
    async _updateStatus(name, status) {
        try {
            await this.watcher.updateStatus(name, (orig) => {
                // avoid unnecessary status updates
                if (orig.status === status) {
                    return;
                }
                log.debug(`Updating status of node resource "${name}"`);
                return {
                    apiVersion: 'openebs.io/v1alpha1',
                    kind: 'MayastorNode',
                    metadata: orig.metadata,
                    spec: orig.spec,
                    status: status,
                };
            });
        }
        catch (err) {
            log.error(`Failed to update status of node resource "${name}": ${err}`);
        }
    }
    // Delete node resource with specified name.
    //
    // @param {string} name   Name of the node resource to delete.
    //
    async _deleteResource(name) {
        try {
            log.info(`Deleting node resource "${name}"`);
            await this.watcher.delete(name);
        }
        catch (err) {
            log.error(`Failed to delete node resource "${name}": ${err}`);
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
}
module.exports = NodeOperator;
