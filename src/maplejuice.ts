import { fileSystemProtocol } from './filesys';
import swimFuture from './swim';
import { ipToID } from './swim';
import * as Swim from 'swim';
import { paxos, leaderStream } from './paxos';
import { Observable } from 'rxjs/Rx';
import { partitionDataset } from './partition-dataset';
import * as Bluebird from 'bluebird';
import * as express from 'express';
import * as crypto from 'crypto';
import * as bodyParser from 'body-parser';
import * as rp from 'request-promise';
import * as mkdirp from 'mkdirp';
import * as rimraf from 'rimraf';
import * as path from 'path';
import * as fs from 'fs';

interface MasterMaple {

}

interface MasterJuice {

}

interface MasterQuery {

}

interface MapleJob {
  type: 'maple';
}

interface JuiceJob {
  type: 'juice';
}

type Command = MapleJob | JuiceJob;

const WorkerPort = 54321;
const MasterPort = 54231;

const intermediateLocation = 'mp_tmp';

/**
 * Maple:
 * 1. Split dataset and upload as `mapleExe_${1..N}`
 * 2. Send maple command to master
 * 3. (Master) schedule workload and query progress every 500ms
 * 
 * Master:
 * Global state consists of a command queue and a pool of tasks. The state is replicated
 * across up to two slaves. When a worker goes down with its assigned task, the master marks that task as
 * waiting. When a worker becomes available, the master schedules waiting tasks on that worker, and marks
 * the task as in progress.
 * 
 * Worker:
 * Maintains a thread that takes maple/juice messages and runs the script.
 * Reads input stream and performs map computation on the fly.
 * Aggregates key-value pairs on local filesystem with key name `intermediatePrefix${urlencode(key)}`.
 * When thread is done with current workload, append results, signal master, and update state.
 * Respond to progress queries with worker state.
 * 
 * Append:
 * Grab lock from master to append to key.
 * Lock granularity is by key.
 * Auto-unlocks after 30 seconds.
 * Only assigned workers may grab locks. Unidentified requests are rejected.
 */

export const maplejuice = Promise.all([paxos, fileSystemProtocol, swimFuture])
.then(async ([paxos, fileSystemProtocol, swim]) => {

  // recreate tmp folder
  await Bluebird.promisify(rimraf)(intermediateLocation);
  await Bluebird.promisify(mkdirp)(intermediateLocation);

  let commandQueue: Command[] = [];

  let ourId = ipToID(swim.whoami());
  // start master program if we are the leader
  leaderStream.subscribe(id => {
    if (id === ourId) {
      startMapleJuiceMaster();
    } else {
      stopMapleJuiceMaster();
    }
  });

  function startMapleJuiceMaster() {
    console.log('Start MapleJuice Master');
  }

  function stopMapleJuiceMaster() {
    console.log('Stop MapleJuice Master');
  }

  let haveLeader = Bluebird.defer();
  leaderStream.skip(1).take(1).do(() => haveLeader.resolve()).subscribe();

  async function masterRequest(api: string, body: MasterMaple | MasterJuice | MasterQuery) {
    await haveLeader.promise;
    let id = paxos().valueOr(1);
    return rp({
      uri: `http://fa16-cs425-g06-${ id < 10 ? '0' + id : id }.cs.illinois.edu:${MasterPort}/${api}`,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: body || {},
      json: true,
      encoding: null,
      gzip: false
    });
  }

  async function withLeader() {
    let defer = Bluebird.defer<number>();
    await haveLeader.promise;
    let leaderM = paxos();
    leaderM.caseOf({
      just: leader => defer.resolve(leader),
      nothing: () => { throw new Error('Cannot schedule while master is down'); }
    });
    return defer;
  }

  async function maple(
    mapleExe: string,
    numMaples: number,
    intermediatePrefix: string,
    sourceDirectory: string)
  {
    let leader = await withLeader();
    // upload maple script
    let mapleScriptName = path.basename(mapleExe, '.js');
    await fileSystemProtocol.put(mapleScriptName, () => fs.createReadStream(mapleExe));
    // split and upload dataset
    let dataStreams = partitionDataset(sourceDirectory, numMaples);
    let datasetPrefix = Math.round(Math.random() * 1000000);
    // TODO: handle upload failure
    await Promise.all(dataStreams.map((stream, index) =>
      fileSystemProtocol.put(`M${datasetPrefix}_${mapleScriptName}_DS${index}`, () => stream)));
    // start job
    await masterRequest('maple', {
      mapleScriptName,
      numMaples,
      datasetPrefix,
      intermediatePrefix
    });
    console.log('Maple job ${mapleScriptName} sent');
  }

  async function juice(
    juiceExe: string,
    numJuices: number,
    intermediatePrefix: string,
    destFilename: string,
    deleteInput: boolean,
    partitionAlgorithm: 'hash' | 'range')
  {
    let leader = await withLeader();
  }

  return { maple, juice };

});
