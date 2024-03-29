import * as express from 'express';
import swimFuture from './swim';
import { ipToID, MemberState } from './swim';
import * as Swim from 'swim';
import * as rp from 'request-promise';
import * as Request from 'request';
import * as Bluebird from 'bluebird';
import * as crypto from 'crypto';
import * as bodyParser from 'body-parser';
import * as fs from 'fs';
import * as mkdirp from 'mkdirp';
import * as stream from 'stream';
import * as rimraf from 'rimraf';
const modexp = require('mod-exp');

const debugFS = true;

const debugLog = (...args) => {
  if (debugFS)
    (<any> console.log)(...args);
};

const storeLocation = 'store';
const writeFile = Bluebird.promisify((f: string, data, cb) => fs.writeFile(f, data, cb));

function* mapItr <T, R> (input: IterableIterator<T>, fn: (x: T) => R) {
  for (let x of input) yield fn(x);
}

/**
 * Removes the duplicate copies of elements in an array
 */
function dedupe <T> (input: T[]): T[] {
  let map = new Map<T, boolean>();
  input.forEach(t => map.set(t, true));
  return Array.from(map.keys());
}

function firstFewSuccess <T> (iterator: IterableIterator<() => Promise<T>>, max: number, curr?: number, lastErr?: any): Promise<void> {
  curr = curr || 0;
  if (curr >= max) return Promise.reject(new Error('Must run at least one iteration'));
  // initial parallelism
  let initial: Promise<T>[] = [];
  for (let i = curr; i < max; i++) {
    let next = iterator.next();
    if (typeof next.value === 'undefined') return Promise.reject(lastErr || new Error('No success'));
    initial.push(next.value());
  }
  let oneMore = (err?: any): Promise<T> => {
    let next = iterator.next();
    if (typeof next.value === 'undefined') return Promise.reject(err || new Error('No success'));
    return next.value().catch(err => oneMore(err));
  }
  return Promise.all(initial)
  .catch(err => oneMore(err))
  .then(vals => vals[0]);
}

function sequentialAttempt <T> (promises: (() => Promise<T>)[] | IterableIterator<() => Promise<T>>): Promise<T> {
  let iterator = promises instanceof Array ? promises[Symbol.iterator]() : promises;
  return firstFewSuccess(iterator, 1);
}

async function request(
  id: number,
  api: string,
  key: string,
  body?: Buffer | (() => stream.Readable),
  writeStreamProvider?: () => stream.Writable): Promise<Buffer> {
  let initialTime: number;
  if (api === 'download') {
    debugLog(`Downloading ${key} from node ${id}`);
    initialTime = new Date().getTime();
  } else if (api === 'upload') {
    debugLog(`Uploading ${key} to node ${id}`);
    initialTime = new Date().getTime();
  } else if (api === 'append') {
    debugLog(`Appending ${key} to node ${id}`);
    initialTime = new Date().getTime();
  }
  let makePromise = () => {
    let maker = writeStreamProvider ? Request : rp;
    let readable = body ? body instanceof Buffer ? undefined : body() : undefined;
    let p = maker({
      uri: `http://fa16-cs425-g06-${ id < 10 ? '0' + id : id }.cs.illinois.edu:22895/${api}`,
      method: 'POST',
      headers: {
        'sdfs-key': key,
        'Content-Type': 'application/octet-stream',
      },
      body: body ? body instanceof Buffer ? body : readable : undefined,
      encoding: null,
      gzip: false
    });
    // handle read failure
    if (readable) readable.on('error', err => p.emit('error', err));
    if (writeStreamProvider) {
      let totalSize = 1;
      let stream = writeStreamProvider();
      let result = Bluebird.defer<number>();
      p.on('data', data => {
        let haveSpace = stream.write(data);
        if (!haveSpace) {
          p.pause();
          stream.once('drain', () => p.resume());
        }
        totalSize += data.length;
      });
      // no need to double report the error
      p.on('error', e => { /* stream.emit('error', e); */ result.reject(e); });
      p.on('end', () => { stream.end(); result.resolve(totalSize); });
      return result.promise;
    } else {
      return <Bluebird<Buffer | number>> <any> p;
    }
  };
  return makePromise()
  .catch(err => {
    // 404 is definitely an error
    if (err.name === 'StatusCodeError' && err.statusCode === 404) throw err;
     // attempt to retry for one more time 
    return Bluebird.delay(30 + Math.random() * 30).then(() => makePromise()); })
  .then(x => {
    let length: number = 0;
    if (api === 'download') {
      if (x instanceof Buffer)
        length = x.length;
      else
        length = x;
    }
    if (api === 'upload' || api === 'append') {
      if (x instanceof Buffer)
        length = JSON.parse(x.toString()).len;
      else
        length = x;
    }
    if (initialTime) {
      debugLog(`Time taken: ${ (new Date().getTime() - initialTime) / 1000 } seconds. Bandwidth: ${
        length / 1024 / 1024 / ((new Date().getTime() - initialTime) / 1000) } MB/s`);
    }
    if (x instanceof Buffer)
      return x;
    else
      return new Buffer('');    // empty buffer when user supplies stream
  });
}

export const fileSystemProtocol = swimFuture.then(async swim => {
  interface Dictionary {
    [key: string]: string;
  }
  const app = express();
  const files: Dictionary = {};
  const inFlightReplication: {[key: string] : boolean} = {};

  function localStorageKey(key: string) {
    const sha1sum = crypto.createHash('sha1');
    sha1sum.update(key);
    return `${storeLocation}/${sha1sum.digest('hex')}`;
  }

  /**
   * probing strategy for a node
   */
  function* hashKey(key: string) {
    const md5sum = crypto.createHash('md5');
    md5sum.update(key);
    let digestHex = md5sum.digest('hex');
    // squash to 16-bit number
    let exponent = 0;
    for (let i = 0; i < 32; i += 4) {
      let part = parseInt(digestHex.substr(i, 4), 16);
      exponent = exponent ^ part;
    }
    // generate probes
    let appeared = {};
    let appearedCount = 0;
    for (let i = 0, j = 0; i < 30; ) {
      // (1543 ^ (exponent + i)) mod 2017
      let candidate: number;
      do {
        candidate = (<number> modexp(1543, exponent + j, 4057)) % 10;
        j += 1;
      } while (appeared[candidate]);
      i += 1;
      appeared[candidate] = true;
      appearedCount += 1;
      if (appearedCount == 10) {
        // reshuffle
        appeared = {};
        appearedCount = 0;
      }
      yield candidate + 1;
    }
  }

  function getActiveMembers() {
    let activeMembersTable: {[key: number]: boolean} = {};
    let activeMembers = swim.members()
      .map(m => ipToID(m.host))
      .filter(x => isNaN(x) !== true);
    activeMembers.forEach(x => activeMembersTable[x] = true);
    return activeMembersTable;
  }


  function* hashKeyActive(key: string) {
    let activeMembers = getActiveMembers();
    for (let x of hashKey(key)) {
      if (activeMembers[x]) yield x;
    }
  }

  /**
   * Returns the nodes that should have this file when all are online.
   */
  function getAllIdealReplicants(key: string): number[] {
    let i = 0;
    let result = [];
    for (let x of hashKey(key)) {
      result.push(x);
      i++;
      if (i >= 3) break;
    }
    return result;
  }

  /**
   * Returns up to 3 online nodes that have this file.
   */
  function getAllActiveReplicants(key: string, assumeOnline?: number): number[] {
    let activeMembersTable = getActiveMembers();
    let i = 0;
    let result = [];
    for (let x of hashKey(key)) {
      if (!activeMembersTable[x] && x !== assumeOnline) continue;
      result.push(x);
      i++;
      if (i >= 3) break;
    }
    return result;
  }

  /**
   * Get the files on that node right before it went down.
   */
  const filesLostOnNode = (id: number) => Object.keys(files)
      .filter(k => getAllActiveReplicants(k, id).findIndex(x => x === id) >= 0);

  // send our file if possible
  app.post('/download', (req, res) => {
    let key = req.header('sdfs-key');
    if (key && files[key]) {
      debugLog(`Node ${ipToID(`${req.connection.remoteAddress}:22895`)} is downloading ${key} from us`);
      let stream = fs.createReadStream(localStorageKey(key));
      stream.pipe(res);
    } else {
      res.status(404).send('Not found: ' + key);
    }
  });

  // receives binary and stores it as a buffer
  app.post('/upload', (req, res) => {
    let key = req.header('sdfs-key');
    if (key) {
      debugLog(`Node ${ipToID(`${req.connection.remoteAddress}:22895`)} is uploading ${key} to us`);
      files[key] = localStorageKey(key);
      let stream = fs.createWriteStream(localStorageKey(key));
      let totalSize = 0;
      req.on('data', data => { stream.write(data); totalSize += data.length; });
      req.on('end', () => {
        stream.end();
        res.status(200).send({
          len: totalSize
        });
      });
    } else {
      res.status(400).send('Must specify sdfs-key');
    }
  });

  // receives binary and appends it into existing buffer
  app.post('/append', (req, res) => {
    let key = req.header('sdfs-key');
    if (key) {
      debugLog(`Node ${ipToID(`${req.connection.remoteAddress}:22895`)} is appending ${key} to us`);
      files[key] = localStorageKey(key);
      let stream = fs.createWriteStream(localStorageKey(key), {
        flags: 'a'
      });
      let totalSize = 0;
      req.on('data', data => { stream.write(data); totalSize += data.length; });
      req.on('end', () => {
        stream.end();
        res.status(200).send({
          len: totalSize
        });
      });
    } else {
      res.status(400).send('Must specify sdfs-key');
    }
  });

  // list all keys under format { keys: [...] }
  app.post('/list_keys', (req, res) => res.send({ keys: Object.keys(files) }));

  // tells us we should replicate this file, when a node goes down
  app.post('/push', (req, res) => {
    let key = req.header('sdfs-key');
    if (key) {
      if (inFlightReplication[key] || files[key]) {
        // ignore
        res.sendStatus(200);
      } else {
        // need to replicate
        debugLog(`Attempt to replicate ${key} due to node failure`);
        inFlightReplication[key] = true;
        sequentialAttempt(getAllActiveReplicants(key)
          .map(id =>
            () => request(id, 'download', key, undefined, () =>
              fs.createWriteStream(localStorageKey(key)))
        ))
        .then(() => {
          files[key] = localStorageKey(key);
          inFlightReplication[key] = false;
          res.sendStatus(200);
        })
        .catch(err =>
          res.status(500).send('On-failure replication errorred: ' + JSON.stringify(err)));
      }
    } else {
      res.status(400).send('Must specify sdfs-key');
    }
  });

  // return if the file is present
  app.post('/query', (req, res) => {
    let key = req.header('sdfs-key');
    if (key) {
      res.send({ present: !!files[key] });
    } else {
      res.status(400).send('Must specify sdfs-key');
    }
  });

  // removes the file from memory
  app.post('/delete', (req, res) => {
    let key = req.header('sdfs-key');
    debugLog(`Node ${ipToID(`${req.connection.remoteAddress}:22895`)} ordering us to delete ${key}`);
    if (key) {
      res.send({ deleted: !!files[key] });
      delete files[key];
    } else {
      res.status(400).send('Must specify sdfs-key');
    }
  });

  const append = async (key: string, file: () => stream.Readable) => {
    let nodesWithKey = await ls(key);
    if (nodesWithKey.length === 0) {
      // this is a new file; treat is as upload
      return await put(key, file);
    } else {
      await Promise.all(nodesWithKey.map(node => request(node, 'append', key, file)));
    }
  };

  const put = (key: string, file: () => stream.Readable) =>
    firstFewSuccess(mapItr(hashKeyActive(key), id => () => request(id, 'upload', key, file)), 3);

  const get = (key: string, writeStreamProvider: () => stream.Writable) =>
    sequentialAttempt(mapItr(hashKeyActive(key), id => () => request(id, 'download', key, undefined, writeStreamProvider)));

  const del = (key: string) => Promise.all(getAllActiveReplicants(key)
    .map(id => request(id, 'delete', key)));

  const ls = async (key: string) => {
    let stored: number[] = [];
    // inserts all servers which has key
    await firstFewSuccess(mapItr(hashKeyActive(key), id => () =>
      request(id, 'query', key)
      .then(resp => JSON.parse(resp.toString()))
      .then(x => x.present ? stored.push(id) : Promise.resolve(NaN))), 10);
    return dedupe(stored);
  };

  const store = () => Promise.resolve(Object.keys(files));

  /**
   * Try super hard during initial replication since the file system may not be set up yet.
   */
  function requestInitial(id: number, api: string, key: string, body?: Buffer): Promise<Buffer> {
    return request(id, api, key, body)
    .catch(err => Bluebird.delay(600 + Math.random() * 300).then(() =>
      request(id, api, key, body)
      .catch(err => Bluebird.delay(1000 + Math.random() * 300).then(() =>
        request(id, api, key, body))
        .catch(err => Bluebird.delay(1500 + Math.random() * 300).then(() =>
          request(id, api, key, body))))));
  }

  return await Bluebird.promisify((p: number, cb) => app.listen(p, cb))(22895)
  .then(() => debugLog('Initial replication'))
  .then(async () => {
    // recreate data folder
    await Bluebird.promisify(rimraf)(storeLocation);
    await Bluebird.promisify(mkdirp)(storeLocation);
  })
  .then(() => {
    // set up global error handlers
    process.on('unhandledRejection', (reason, promise) => {
      promise.catch(err => setTimeout(() => {
        let errString = 'Warning: Unhandled Promise Rejection, reason: ' + reason + ', stack: ' + err.stack;
        if (err.error && err.response) {
          errString += '\n    - Request-Promise Error: ' + err.error.toString();
          errString += '\n    - Request-Promise Response: ' + JSON.stringify(err.response.body);
        }
        console.warn(errString);
      }, 10));
    });
    process.on('rejectionHandled', () => { });
  })
  .then(() => Bluebird.delay(50).then(() => Promise.all(  // perform initial replication
      Object.keys(getActiveMembers())
      .filter(id => +id !== ipToID(swim.whoami()))    // we're not active yet
      .map(id => requestInitial(+id, 'list_keys', '')
      .then(resp => JSON.parse(resp.toString()))
      .catch(err => ({ keys: [] }))   // some nodes may go down, we assume they have nothing
      .then <[number, string[]]> (obj => [+id, obj.keys]))))
    .then(allKeys => {
      // group by key
      let keyToNodes: { [key: string]: number[] } = {};
      allKeys.forEach(([id, keys]) =>
        keys.forEach(key => {
          keyToNodes[key] = keyToNodes[key] || [];
          keyToNodes[key].push(id);
        }));
      // find the ones we own
      let ourID = ipToID(swim.whoami());
      return Promise.all(
        Object.keys(keyToNodes)
        .filter(k => getAllIdealReplicants(k).findIndex(x => x === ourID) >= 0)
        .map(k =>
          sequentialAttempt(keyToNodes[k]   // try all nodes which have k
            .map(id => () => requestInitial(id, 'download', k)))   // replicate
          .then(buf => writeFile(localStorageKey(k), buf))
          .then(() => files[k] = localStorageKey(k))))
      .then(() => Promise.all(   // delete extra replica
        Object.keys(files).map(k => {
          keyToNodes[k].push(ourID);
          let prs = [];
          if (keyToNodes[k].length > 3) {
            // delete down to 3
            let ideal = getAllIdealReplicants(k);
            let extraNodes = keyToNodes[k].filter(id => ideal.findIndex(x => x === id) < 0);
            for (let i = 0; i < keyToNodes[k].length - 3; i++) {
              let idx = Math.floor(Math.random() * extraNodes.length) | 0;
              if (idx >= extraNodes.length) idx = extraNodes.length - 1;
              if (idx < 0) break;
              // delete file
              prs.push(requestInitial(extraNodes[idx], 'delete', k));
              extraNodes = extraNodes.filter((_, j) => j !== idx);
            }
          }
          return Promise.all(prs);
        })));
    }))
  .catch(err => {
    console.error(`Failed to replicate`);
    setTimeout(() => process.exit(-1), 50);
  })
  .then(() =>   // enable on-failure replication
    swim.on(Swim.EventType.Change, update => {
      if (update.state === MemberState.Faulty) {
        // tell the right guy to replicate
        let activeMembers = getActiveMembers();
        let downID = ipToID(update.host);
        filesLostOnNode(downID).forEach(key => {
          // find the backup replicant
          let replicant: number = NaN;
          let nodesWithFile = getAllActiveReplicants(key, downID);
          for (let id of hashKey(key)) {
            // find the next active node in the probe sequence which does not have this file
            if (activeMembers[id] && nodesWithFile.findIndex(x => x === id) < 0) {
              replicant = id;
              break;
            }
          }
          if (isNaN(replicant) !== true) {
            // instruct replicant to grab file
            debugLog(`Requesting ${replicant} to backup ${key}`);
            request(replicant, 'push', key)
            .catch(err => {
              console.error('Push failure: ', JSON.stringify(err));
              // try again
              request(replicant, 'push', key);
            });
          } else {
            debugLog(`Cannot find on-failure replication candidate for ${key}`);
          }
        });
      }
    }))
  .then(() => Object.freeze({
    put, get, del, ls, store, append
  }));
});
