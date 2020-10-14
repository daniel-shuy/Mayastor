// Implementation of a cache for arbitrary k8s custom resource in openebs.io
// api with v1alpha1 version.

import * as _ from 'lodash';
import sleep from 'sleep-promise';
import {
  CustomObjectsApi,
  KubeConfig,
  KubernetesObject,
  KubernetesListObject,
  ListWatch,
  V1ListMeta,
  Watch,
} from '@kubernetes/client-node';

const EventEmitter = require('events');
const log = require('./logger').Logger('watcher');

// If listWatch errors out then we restart it after this many msecs.
const RESTART_DELAY: number = 3000;
// We wait this many msecs for an event confirming operation done previously.
const EVENT_TIMEOUT: number = 5000;
const GROUP: string = 'openebs.io';
const VERSION: string = 'v1alpha1';

// Commonly used metadata attributes.
export class CustomResourceMeta extends V1ListMeta {
  name?: string;
  namespace?: string;
  generation?: number;
  finalizers?: string[];
}

// Properties of custom resources (all optional so that we can do easy
// conversion from "object" type)
export class CustomResource implements KubernetesObject {
  apiVersion?: string;
  kind?: string;
  metadata?: CustomResourceMeta;
  spec?: object;
  status?: any;
}

// Resource cache keeps track of a k8s custom resource and exposes methods
// for modifying the cache content.
//
// It is a classic operator loop design as seen in i.e. operator-sdk (golang)
// to watch a k8s resource. We utilize k8s client library to take care of low
// level details.
//
// It is a general implementation of watcher which can be used for any resource
// operator. The operator should subscribe to "new", "mod" and "del" events that
// are triggered when a resource is added, modified or deleted.
export class CustomResourceCache<T> extends EventEmitter {
  name: string;
  plural: string;
  namespace: string;
  waiting: Record<string, () => void>;
  k8sApi: CustomObjectsApi;
  listWatch: ListWatch<CustomResource>;
  creator: new (obj: CustomResource) => T;
  eventHandlers: Record<string, (obj: CustomResource) => void>;
  connected: boolean;
  restartDelay: number;
  eventTimeout: number;

  // Create the cache for given namespace and resource name.
  //
  // @param namespace   Namespace of custom resource.
  // @param name        Name of the resource.
  // @param kubeConfig  Kube config object.
  // @param creator     Constructor of the object from custom resource object.
  // @param opts        Cache/watcher options.
  constructor(
    namespace: string,
    name: string,
    kubeConfig: KubeConfig,
    creator: new (obj: CustomResource) => T,
    opts?: {
      restartDelay: number,
      eventTimeout: number
    }
  ) {
    super();
    this.k8sApi = kubeConfig.makeApiClient(CustomObjectsApi);
    this.name = name;
    this.plural = name + 's';
    this.namespace = namespace;
    this.creator = creator;
    this.waiting = {};
    this.connected = false;
    this.restartDelay = opts?.restartDelay || RESTART_DELAY;
    this.eventTimeout = opts?.eventTimeout || EVENT_TIMEOUT;
    this.eventHandlers = {
      add: this._onEvent.bind(this, 'new'),
      update: this._onEvent.bind(this, 'mod'),
      delete: this._onEvent.bind(this, 'del'),
    };

    const watch = new Watch(kubeConfig);
    this.listWatch = new ListWatch<CustomResource>(
      `/apis/${GROUP}/${VERSION}/namespaces/${this.namespace}/${this.plural}`,
      watch,
      async () => {
        var resp = await this.k8sApi.listNamespacedCustomObject(
          GROUP,
          VERSION,
          this.namespace,
          this.plural);
        return {
          response: resp.response,
          body: resp.body as KubernetesListObject<CustomResource>,
        };
      },
      false
    );
  }

  // Called upon a watcher event. It unblocks create or update operation if any
  // is waiting for the event and propagates the event further.
  _onEvent(event: string, cr: CustomResource) {
    let name = cr.metadata?.name;
    if (name === undefined) {
      log.error(`Ignoring event ${event} with object without a name`);
      return;
    }
    let cb = this.waiting[name];
    if (cb !== undefined) {
      delete this.waiting[name];
      cb();
    }
    this._doWithObject(cr, (obj) => this.emit(event, obj));
  }

  // Convert custom resource object to desired object swallowing exceptions
  // and call callback with the new object.
  _doWithObject(obj: CustomResource | undefined, cb: (obj: T) => void): void {
    if (obj === undefined) return;

    try {
      var newObj = new this.creator(obj);
    } catch (e) {
      log.error(`Ignoring invalid ${this.name} custom resource: ${e}`);
      return;
    }
    cb(newObj);
  }

  // This method does not return until the cache is successfully populated.
  async start() {
    for (let evName in this.eventHandlers) {
      this.listWatch.on(evName, this.eventHandlers[evName]);
    }
    while (true) {
      try {
        await this.listWatch.start();
        break;
      } catch (err) {
        log.error(`Failed to start ${this.name} watcher: ${err}`)
        log.info(`Restarting ${this.name} watcher after ${this.restartDelay}ms...`);
        await sleep(this.restartDelay);
      }
    }
    this.connected = true;
    this.listWatch.on('error', this._onError.bind(this));
    log.trace(`Initial content of the "${this.name}" cache: ` +
      this.listWatch.list().map((i: CustomResource) => i.metadata?.name));
  }

  // Called when the connection gets broken.
  _onError(err: any) {
    this.listWatch.off('error', this._onError);
    this.stop();
    this.connected = false;
    log.error(`Watcher error: ${err}`);
    log.info(`Restarting ${this.name} watcher after ${this.restartDelay}ms...`);
    setTimeout(() => this.start(), this.restartDelay);
  }

  // Deregister all internal event handlers on the watcher.
  stop() {
    log.debug(`Deregistering "${this.name}" cache event handlers`);
    for (let evName in this.eventHandlers) {
      this.listWatch.off(evName, this.eventHandlers[evName]);
    }
  }

  isConnected(): boolean {
    // XXX should we propagate event to consumers about the reset?
    return this.connected;
  }

  // Get all objects from the cache.
  list(): T[] {
    let list: T[] = [];
    this.listWatch.list().forEach((item) => {
      this._doWithObject(item, (obj) => list.push(obj));
    });
    return list;
  }

  // Get object with given name (ID).
  get(name: string): T | undefined {
    var result;
    this._doWithObject(this.listWatch.get(name), (obj) => result = obj);
    return result;
  }

  // Create the resource and wait for it to be created.
  create(obj: CustomResource): Promise<void> {
    let name: string = obj.metadata?.name || '';
    if (!name) {
      throw Error("Object does not have a name");
    }
    log.trace(`Creating new "${this.name}" resource: ${JSON.stringify(obj)}`);
    return this.k8sApi.createNamespacedCustomObject(
      GROUP,
      VERSION,
      this.namespace,
      this.plural,
      obj
    ).then(this._waitForEvent.bind(this, name));
  }

  // Do not return until we receive an event from watcher. Otherwise the
  // object in the cache might be stale when we do the next modification to it.
  // Set timeout for the case when we never receive the event.
  _waitForEvent(name: string): Promise<void> {
    return new Promise((resolve, _reject) => {
      const since = (new Date()).getTime();
      const timer = setTimeout(() => {
        delete this.waiting[name];
        const delta = (new Date()).getTime() - since;
        log.warn(`Timed out waiting for watcher event on ${this.name} "${name}" (${delta}ms)`);
        resolve();
      }, this.eventTimeout);

      this.waiting[name] = () => {
        const delta = (new Date()).getTime() - since;
        log.trace(`The operation on ${this.name} "${name}" took ${delta}ms`);
        clearTimeout(timer);
        delete this.waiting[name];
        resolve();
      };
    });

  }
  // Update the resource. The merge callback takes the original version from
  // the cache, modifies it and returns the new version of object. The reason
  // for this is that sometimes we get stale errors and we must repeat
  // the operation with an updated version of the original object.
  async update(name: string, merge: (orig: T) => CustomResource | undefined) {
    let orig = this.get(name);
    if (orig === undefined) {
      log.warn(`Tried to update ${this.name} "${name}" but it is gone`)
      return;
    }
    let obj = merge(orig);
    await this._update(name, obj);
  }

  // Same as above but works with custom resource type rather than user
  // defined object.
  async _updateCustomResource(name: string, merge: (orig: CustomResource) => CustomResource | undefined) {
    let orig = this.listWatch.get(name);
    if (orig === undefined) {
      log.warn(`Tried to update to ${this.name} "${name}" that does not exist`);
      return;
    }
    let obj = merge(orig);
    await this._update(name, obj);
  }

  // Update the resource and wait for mod event or silently timeout.
  _update(name: string, obj: CustomResource | undefined): Promise<void> {
    if (obj === undefined) {
      log.trace(`Skipping update of ${this.name} "${name}" - it is the same`)
      return new Promise((resolve) => resolve());
    }
    log.trace(`Updating ${this.name} "${name}": ${JSON.stringify(obj)}`);
    return this.k8sApi.replaceNamespacedCustomObject(
      GROUP,
      VERSION,
      this.namespace,
      this.plural,
      name,
      obj
    ).then(this._waitForEvent.bind(this, name));
  }

  // Update status of the resource. Unlike in case create/update we don't have
  // to wait for confirming event because generation number is not incremented
  // upon status change.
  async updateStatus(name: string, merge: (orig: T) => CustomResource | undefined) {
    let orig = this.get(name);
    if (orig === undefined) {
      log.warn(`Tried to update status of ${this.name} "${name}" but it is gone`);
      return;
    }
    let obj = merge(orig);
    if (obj === undefined) {
      // likely means that the props are the same - nothing to do
      return;
    }
    log.trace(`Updating status of ${this.name} "${name}": ${JSON.stringify(obj.status)}`);
    await this.k8sApi.replaceNamespacedCustomObjectStatus(
      GROUP,
      VERSION,
      this.namespace,
      this.plural,
      name,
      obj
    );
  }

  // Delete the resource.
  delete(name: string): Promise<void> {
    let orig = this.get(name);
    if (orig === undefined) {
      log.warn(`Tried to delete ${this.name} "${name}" that does not exist`);
      return new Promise((resolve) => resolve());
    }
    log.trace(`Deleting ${this.name} "${name}"`);
    return this.k8sApi.deleteNamespacedCustomObject(
      GROUP,
      VERSION,
      this.namespace,
      this.plural,
      name
    ).then(this._waitForEvent.bind(this, name));
  }

  // Add finalizer to given resource if not already there.
  async addFinalizer(name: string, finalizer: string) {
    await this._updateCustomResource(name, (orig) => {
      let finalizers = orig.metadata?.finalizers;
      let newFinalizers = finalizers || [];
      if (newFinalizers.indexOf(finalizer) >= 0) {
        // it's already there
        return;
      }
      newFinalizers = [finalizer].concat(newFinalizers);
      let obj = _.cloneDeep(orig);
      if (obj.metadata === undefined) {
        throw new Error(`Resource ${this.name} "${name}" without metadata`)
      }
      obj.metadata.finalizers = newFinalizers;
      return obj;
    });
  }

  // Remove finalizer from the resource in case it's there.
  async removeFinalizer(name: string, finalizer: string) {
    await this._updateCustomResource(name, (orig) => {
      let finalizers = orig.metadata?.finalizers;
      let newFinalizers = finalizers || [];
      let idx = newFinalizers.indexOf(finalizer);
      if (idx < 0) {
        // it's not there
        return;
      }
      newFinalizers.splice(idx, 1);
      let obj = _.cloneDeep(orig);
      if (obj.metadata === undefined) {
        throw new Error(`Resource ${this.name} "${name}" without metadata`)
      }
      obj.metadata.finalizers = newFinalizers;
      return obj;
    });
  }
}