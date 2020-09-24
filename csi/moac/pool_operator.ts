// Pool operator monitors k8s pool resources (desired state). It creates
// and destroys pools on storage nodes to reflect the desired state.

import assert from 'assert';
import * as fs from 'fs';
import * as _ from 'lodash';
import * as path from 'path';
import {
  ApiextensionsV1Api,
  KubeConfig,
} from '@kubernetes/client-node';
import {
  CustomResource,
  CustomResourceCache,
  CustomResourceMeta,
} from './cache';

const yaml = require('js-yaml');
const log = require('./logger').Logger('pool-operator');
const EventStream = require('./event_stream');
const Workq = require('./workq');

const RESOURCE_NAME: string = 'mayastorpool';
const POOL_FINALIZER = 'finalizer.mayastor.openebs.io';

// Load custom resource definition
const crdPool = yaml.safeLoad(
  fs.readFileSync(path.join(__dirname, '/crds/mayastorpool.yaml'), 'utf8')
);

enum PoolState {
  Unknown = "unknown",
  Online = "online",
  Offline = "offline",
  Degraded = "degraded",
  Pending = "pending",
  Failed = "failed",
}

function poolStateFromString(val: string): PoolState {
  if (val === PoolState.Online) {
    return PoolState.Online;
  } else if (val === PoolState.Offline) {
    return PoolState.Offline;
  } else if (val === PoolState.Degraded) {
    return PoolState.Degraded;
  } else if (val === PoolState.Pending) {
    return PoolState.Pending;
  } else if (val === PoolState.Failed) {
    return PoolState.Failed;
  } else {
    return PoolState.Unknown;
  }
}

// Object defines properties of pool resource.
class PoolResource extends CustomResource {
  apiVersion?: string;
  kind?: string;
  metadata: CustomResourceMeta;
  spec: {
    node: string,
    disks: string[],
  };
  status: {
    state: string,
    reason?: string,
    disks?: string[],
    capacity?: number,
    used?: number
  };

  constructor(cr: CustomResource) {
    super();
    this.apiVersion = cr.apiVersion;
    this.kind = cr.kind;
    if (cr.metadata === undefined) {
      throw new Error('missing metadata');
    } else {
      this.metadata = cr.metadata;
    }
    if (cr.spec === undefined) {
      throw new Error('missing spec');
    } else {
      let node = (cr.spec as any).node;
      if (typeof node !== 'string') {
        throw new Error('missing or invalid node in spec');
      }
      let disks = (cr.spec as any).disks;
      if (!Array.isArray(disks)) {
        throw new Error('missing or invalid disks in spec');
      }
      disks = disks.slice(0).sort();
      //if (typeof disks !== 'string') {
      this.spec = { node, disks };
    }
    this.status = {
      state: poolStateFromString(cr.status?.state),
      reason: cr.status?.reason,
      disks: cr.status?.disks,
      capacity: cr.status?.capacity,
      used: cr.status?.used,
    };
  }

  getName(): string {
    if (this.metadata.name === undefined) {
      throw Error("Resource object does not have a name")
    } else {
      return this.metadata.name;
    }
  }
}

// Pool operator tries to bring the real state of storage pools on mayastor
// nodes in sync with mayastorpool custom resources in k8s.
class PoolOperator {
  namespace: string;
  watcher: CustomResourceCache<PoolResource>; // k8s resource watcher for pools
  registry: any; // registry containing info about mayastor nodes
  eventStream: any; // A stream of node and pool events.
  workq: any; // for serializing pool operations

  // Create pool operator.
  //
  // @param namespace   Namespace the operator should operate on.
  // @param kubeConfig  KubeConfig.
  // @param registry    Registry with node objects.
  constructor (namespace: string, kubeConfig: KubeConfig, registry: any) {
    this.namespace = namespace;
    this.registry = registry; // registry containing info about mayastor nodes
    this.eventStream = null; // A stream of node and pool events.
    this.workq = new Workq(); // for serializing pool operations
    this.watcher = new CustomResourceCache(
      this.namespace,
      RESOURCE_NAME,
      kubeConfig,
      PoolResource
    );
  }

  // Create pool CRD if it doesn't exist.
  //
  // @param kubeConfig  KubeConfig.
  async init (kubeConfig: KubeConfig) {
    log.info('Initializing pool operator');
    let k8sExtApi = kubeConfig.makeApiClient(ApiextensionsV1Api);
    try {
      await k8sExtApi.createCustomResourceDefinition(crdPool);
      log.info(`Created CRD ${RESOURCE_NAME}`);
    } catch (err) {
      // API returns a 409 Conflict if CRD already exists.
      if (err.statusCode !== 409) throw err;
    }
  }

  // Start pool operator's watcher loop.
  //
  // NOTE: Not getting the start sequence right can have catastrophic
  // consequence leading to unintended pool destruction and data loss
  // (i.e. when node info is available before the pool CRD is).
  //
  // The right order of steps is:
  //   1. Get pool resources
  //   2. Get info about pools on storage nodes
  async start () {
    var self = this;

    // get pool k8s resources for initial synchronization and install
    // event handlers to follow changes to them.
    await self.watcher.start();
    self._bindWatcher(self.watcher);

    // this will start async processing of node and pool events
    self.eventStream = new EventStream({ registry: self.registry });
    self.eventStream.on('data', async (ev: any) => {
      if (ev.kind === 'pool') {
        await self.workq.push(ev, self._onPoolEvent.bind(self));
      } else if (ev.kind === 'node' && (ev.eventType === 'sync' || ev.eventType === 'mod')) {
        await self.workq.push(ev.object.name, self._onNodeSyncEvent.bind(self));
      } else if (ev.kind === 'replica' && (ev.eventType === 'new' || ev.eventType === 'del')) {
        await self.workq.push(ev, self._onReplicaEvent.bind(self));
      }
    });
  }

  // Handler for new/mod/del pool events
  //
  // @param {object} ev       Pool event as received from event stream.
  //
  async _onPoolEvent (ev: any) {
    const name: string = ev.object.name;
    const resource = this.watcher.get(name);

    log.debug(`Received "${ev.eventType}" event for pool "${name}"`);

    if (ev.eventType === 'new') {
      if (resource === undefined) {
        log.warn(`Unknown pool "${name}" will be destroyed`);
        await this._destroyPool(name);
      } else {
        await this._updateResource(ev.object);
      }
    } else if (ev.eventType === 'mod') {
      await this._updateResource(ev.object);
    } else if (ev.eventType === 'del' && resource) {
      log.warn(`Recreating destroyed pool "${name}"`);
      await this._createPool(resource);
    }
  }

  // Handler for node sync event.
  //
  // Either the node is new or came up after an outage - check that we
  // don't have any pending pools waiting to be created on it.
  //
  // @param {string} nodeName    Name of the new node.
  //
  async _onNodeSyncEvent (nodeName: string) {
    log.debug(`Syncing pool records for node "${nodeName}"`);

    const resources = this.watcher.list().filter(
      (ent) => ent.spec.node === nodeName
    );
    for (let i = 0; i < resources.length; i++) {
      await this._createPool(resources[i]);
    }
  }

  // Handler for new/del replica events
  //
  // @param {object} ev       Replica event as received from event stream.
  //
  async _onReplicaEvent (ev: any) {
    const pool = ev.object.pool;
    await this._updateFinalizer(pool.name, pool.replicas.length > 0);
  }

  // Stop the events, destroy event stream and reset resource cache.
  stop () {
    this.watcher.removeAllListeners();
    if (this.eventStream) {
      this.eventStream.destroy();
      this.eventStream = null;
    }
  }

  // Bind watcher's new/mod/del events to pool operator's callbacks.
  //
  // @param watcher   k8s pool resource watcher.
  //
  _bindWatcher (watcher: CustomResourceCache<PoolResource>) {
    var self = this;
    watcher.on('new', (resource: PoolResource) => {
      self.workq.push(resource, self._createPool.bind(self));
    });
    watcher.on('mod', (resource: PoolResource) => {
      self.workq.push(resource, self._modifyPool.bind(self));
    });
    watcher.on('del', (resource: PoolResource) => {
      self.workq.push(resource, self._destroyPool.bind(self));
    });
  }

  // Create a pool according to the specification.
  // That includes parameters checks, node lookup and a call to registry
  // to create the pool.
  //
  // @param resource       Pool resource properties.
  //
  async _createPool (resource: PoolResource) {
    const name: string = resource.getName();
    const nodeName = resource.spec.node;

    let pool = this.registry.getPool(name);
    if (pool) {
      // the pool already exists, just update its properties in k8s
      await this._updateResource(pool);
      return;
    }

    const node = this.registry.getNode(nodeName);
    if (!node) {
      const msg = `mayastor does not run on node "${nodeName}"`;
      log.error(`Cannot create pool "${name}": ${msg}`);
      await this._updateResourceProps(name, PoolState.Pending, msg);
      return;
    }
    if (!node.isSynced()) {
      log.debug(
        `The pool "${name}" will be synced when the node "${nodeName}" is synced`
      );
      return;
    }

    // We will update the pool status once the pool is created, but
    // that can take a time, so set reasonable default now.
    await this._updateResourceProps(name, PoolState.Pending, 'Creating the pool');

    try {
      // pool resource props will be updated when "new" pool event is emitted
      pool = await node.createPool(name, resource.spec.disks);
    } catch (err) {
      log.error(`Failed to create pool "${name}": ${err}`);
      await this._updateResourceProps(name, PoolState.Failed, err.toString());
    }
  }

  // Remove the pool from internal state and if it exists destroy it.
  // Does not throw - only logs an error.
  //
  // @param {string} name   Name of the pool to destroy.
  //
  async _destroyPool (name: string) {
    var pool = this.registry.getPool(name);

    if (pool) {
      try {
        await pool.destroy();
      } catch (err) {
        log.error(`Failed to destroy pool "${name}@${pool.node.name}": ${err}`);
      }
    }
  }

  // Changing pool parameters is actually not supported. However the pool
  // operator's state should reflect the k8s state, so we make the change
  // only at operator level and log a warning message.
  //
  // @param {string} newPool   New pool parameters.
  //
  async _modifyPool (resource: PoolResource) {
    const name = resource.getName();
    const pool = this.registry.getPool(name);
    if (!pool) {
      log.warn(`Ignoring modification to unknown pool "${name}"`);
      return;
    }
    if (!_.isEqual(pool.disks, resource.spec.disks)) {
      // TODO: Growing pools, mirrors, etc. is currently unsupported.
      log.error(`Changing disks of the pool "${name}" is not supported`);
    }
    // Changing node implies destroying the pool on the old node and recreating
    // it on the new node that is destructive action -> unsupported.
    if (pool.node.name !== resource.spec.node) {
      log.error(`Moving pool "${name}" between nodes is not supported`);
    }
  }

  // Update status properties of k8s resource to be aligned with pool object
  // properties.
  //
  // NOTE: This method does not throw if the update fails as there is nothing
  // we can do if it fails. Though it logs an error message.
  //
  // @param {object} pool      Pool object.
  //
  async _updateResource (pool: any) {
    var name = pool.name;
    var resource = this.watcher.get(name);

    // we don't track this pool so we cannot update the CRD
    if (!resource) {
      log.warn(`State of unknown pool "${name}" has changed`);
      return;
    }
    var state = poolStateFromString(
      pool.state.replace(/^POOL_/, '').toLowerCase()
    );
    var reason;
    if (state === PoolState.Offline) {
      reason = `mayastor does not run on the node "${pool.node}"`;
    }

    await this._updateResourceProps(
      name,
      state,
      reason,
      pool.disks,
      pool.capacity,
      pool.used,
    );
    await this._updateFinalizer(name, pool.replicas.length > 0);
  }

  // Update status properties of k8s CRD object.
  //
  // Parameters "name" and "state" are required, the rest is optional.
  //
  // NOTE: This method does not throw if the update fails as there is nothing
  // we can do if it fails. Though we log an error message in such a case.
  //
  // @param name       Name of the pool.
  // @param state      State of the pool.
  // @param [reason]   Reason describing the root cause of the state.
  // @param [disks]    Disk URIs.
  // @param [capacity] Capacity of the pool in bytes.
  // @param [used]     Used bytes in the pool.
  async _updateResourceProps (
    name: string,
    state: PoolState,
    reason?: string,
    disks?: string[],
    capacity?: number,
    used?: number,
  ) {
    try {
      await this.watcher.updateStatus(name, (orig: PoolResource) => {
        // avoid the update if the object has not changed
        if (
          state === orig.status.state &&
          reason === orig.status.reason &&
          capacity === orig.status.capacity &&
          used === orig.status.used &&
          _.isEqual(disks, orig.status.disks)
        ) {
          return;
        }

        log.debug(`Updating properties of pool resource "${name}"`);
        let resource: PoolResource = _.cloneDeep(orig);
        resource.status = {
          state: state,
          reason: reason || '',
          disks: disks || [],
        };
        if (capacity != null) {
          resource.status.capacity = capacity;
        }
        if (used != null) {
          resource.status.used = used;
        }
        return resource;
      });
    } catch (err) {
      log.error(`Failed to update status of pool "${name}": ${err}`);
    }
  }

  // Place or remove finalizer from pool resource.
  //
  // @param name       Name of the pool.
  // @param [busy]     At least one replica on it.
  async _updateFinalizer(name: string, busy: boolean) {
    try {
      if (busy) {
        this.watcher.addFinalizer(name, POOL_FINALIZER);
      } else {
        this.watcher.removeFinalizer(name, POOL_FINALIZER);
      }
    } catch (err) {
      log.error(`Failed to update finalizer on pool "${name}": ${err}`);
    }
  }
}

module.exports = PoolOperator;
