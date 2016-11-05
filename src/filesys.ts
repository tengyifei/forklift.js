import * as express from 'express';
import swimFuture from './swim';
import { ipToID, MemberState } from './swim';
import * as Swim from 'swim';
import * as rp from 'request-promise';
import * as Bluebird from 'bluebird';
import * as crypto from 'crypto';
const modexp = require('mod-exp');

function sequentialAttempt <T> (promises: (() => Promise<T>)[]): Promise<T> {
  if (promises.length === 0) return Promise.reject(new Error('No success'));
  return promises[0]()
  .catch(err => sequentialAttempt(promises.slice(1)));
}

function request(id: number, api: string, key: string, body?: Buffer): Promise<Buffer> {
  return rp({
    uri: `fa16-cs425-g06-${ id < 10 ? '0' + id : id }.cs.illinois.edu/${api}`,
    method: 'POST',
    headers: { 'sdfs-key': key },
    body
  });
}

export const fileSystemProtocol = swimFuture.then(async swim => {
  interface Dictionary {
    [key: string]: Buffer;
  }
  const app = express();
  const files: Dictionary = {};
  const inFlightReplication: {[key: string] : boolean} = {};

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
    for (let i = 0; i < 30; i++) {
      // (7 ^ (exponent + i)) mod 10
      yield <number> modexp(7, exponent + i, 10);
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
      res.send(files[key]);
    } else {
      res.sendStatus(404).send('Not found: ' + key);
    }
  });

  // receives binary and stores it as a buffer
  app.post('/upload', (req, res) => {
    let key = req.header('sdfs-key');
    if (key) {
      files[key] = new Buffer(req.body);
    } else {
      res.sendStatus(400).send('Must specify sdfs-key');
    }
  });

  // tells us we should replicate this file, when a node goes down
  app.post('/push', (req, res) => {
    let key = req.header('sdfs-key');
    if (key) {
      if (inFlightReplication[key] || files[key]) {
        // ignore
        res.sendStatus(200);
      } else {
        // need to replicate
        inFlightReplication[key] = true;
        sequentialAttempt(getAllActiveReplicants(key)
          .map(id =>
            () => request(id, 'download', key)
        ))
        .then(content => {
          files[key] = content;
          inFlightReplication[key] = false;
          res.sendStatus(200);
        })
        .catch(err =>
          res.sendStatus(500).send('On-failure replication failed: ' + JSON.stringify(err)));
      }
    } else {
      res.sendStatus(400).send('Must specify sdfs-key');
    }
  });

  // return if the file is present
  app.post('/query', (req, res) => {
    let key = req.header('sdfs-key');
    if (key) {
      res.send({ present: !!files[key] });
    } else {
      res.sendStatus(400).send('Must specify sdfs-key');
    }
  });

  // removes the file from memory
  app.post('/delete', (req, res) => {
    let key = req.header('sdfs-key');
    if (key) {
      res.send({ deleted: !!files[key] });
      delete files[key];
    } else {
      res.sendStatus(400).send('Must specify sdfs-key');
    }
  });

  let put = (key: string, file: Buffer) => new Promise((resolve, reject) => {

  });

  let get = (key: string) => new Promise((resolve, reject) => {

  });

  let del = (key: string) => new Promise((resolve, reject) => {

  });

  let ls = (key: string) => new Promise((resolve, reject) => {

  });

  let store = () => Promise.resolve(Object.keys(files));

  swim.on(Swim.EventType.Change, update => {
    if (update.state === MemberState.Faulty) {
      // tell the right guy to replicate
      let activeMembers = getActiveMembers();
      let downID = ipToID(update.host);
      filesLostOnNode(downID).forEach(key => {
        // find the backup replicant
        let replicant: number = NaN;
        let afterDown = false;
        for (let id of hashKey(key)) {
          if (afterDown && activeMembers[id]) {
            replicant = id;
            break;
          }
          if (id === downID) {
            afterDown = true;
          }
        }
        if (isNaN(replicant) !== true) {
          // instruct replicant to grab file
          request(replicant, 'push', key);
        }
      });
    }
  });

  return await (<(port: number) => Bluebird<{}>> Bluebird.promisify(app.listen, { context: app }))(22895)
  .then(() => {
    // perform initial replication

  })
  .then(() => Object.freeze({
    put, get, del, ls, store
  }));
});
