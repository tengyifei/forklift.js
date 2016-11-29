import { fileSystemProtocol } from './filesys';
import swimFuture from './swim';
import { ipToID } from './swim';
import { paxos, leaderStream } from './paxos';
import { Observable } from 'rxjs/Rx';
import { partitionDataset } from './partition-dataset';
import { ReactiveQueue } from './producer-consumer';
import { makeid, BufferingStream } from './utils';
import { maple as runMaple } from './worker';
import Semaphore from './semaphore';
import * as http from 'http';
import * as Swim from 'swim';
import * as Bluebird from 'bluebird';
import * as express from 'express';
import * as crypto from 'crypto';
import * as bodyParser from 'body-parser';
import * as rp from 'request-promise';
import * as mkdirp from 'mkdirp';
import * as rimraf from 'rimraf';
import * as path from 'path';
import * as fs from 'fs';
import * as stream from 'stream';
import * as _ from 'lodash';
const streamToPromise: (x: stream.Writable) => Promise<void> = require('stream-to-promise');

interface MasterMaple {

}

interface MasterJuice {

}

interface MasterQuery {

}

interface MasterLock {
  key: string;
}

/**
 * Job definition for maple
 */
interface MapleJob {
  type: 'maple';
  id: string;
  taskIds: string[];
  numWorkers: number;
  scriptName: string;
  datasetPrefix: string;
  intermediatePrefix: string;
}

/**
 * Job definition for juice
 */
interface JuiceJob {
  type: 'juice';
  id: string;
  taskIds: string[];
  numWorkers: number;
  scriptName: string;
}

type Command = MapleJob | JuiceJob;

interface TaskBase {
  type: 'mapletask' | 'juicetask';
  jobId: string;
  id: string;
  scriptName: string;
  assignedWorker?: number;
  state: 'waiting' | 'progress';
}

interface MapleTask extends TaskBase {
  type: 'mapletask';
  intermediatePrefix: string;
  inputFile: string;
}

interface JuiceTask extends TaskBase {
  type: 'juicetask';
  inputKeys: string[];
}

type Task = MapleTask | JuiceTask;

interface WorkerQuery {

}

const WorkerPort = 54321;
const MasterPort = 54777;

const intermediateLocation = 'mp_tmp';

/**
 * Maple:
 * 1. Split dataset and upload as `mapleExe_${1..N}`
 * 2. Send maple command to master
 * 3. (Master) schedule workload and query progress every 300ms
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

  ///
  /// worker server
  ///
  let currentTask: Task;

  // recreate tmp folder
  await Bluebird.promisify(rimraf)(intermediateLocation);
  await Bluebird.promisify(mkdirp)(intermediateLocation);

  const workerApp = express();
  workerApp.use(bodyParser.json());

  workerApp.post('/mapleTask', async (req, res) => {
    if (!req.body) return res.sendStatus(400);
    let task: Task = req.body;
    if (currentTask) {
      return res.status(400).send('Task ${current.id} in progress');
    }
    if (task.type !== 'mapletask') {
      return res.status(400).set('Not a Maple task');
    }
    // start
    currentTask = task;
    res.status(200).set('Working');
    // download maple script
    console.log(`Maple: Downloading script ${task.scriptName}`);
    let mapleScript: string;
    await fileSystemProtocol.get(task.scriptName, () => {
      let s = new BufferingStream();
      s.on('finish', () => { mapleScript = s.result.toString(); });
      return s;
    });
    let writes: Promise<void>[] = [];
    // download our part of the dataset
    let inputFile = task.inputFile;
    console.log(`Maple: Downloading partition ${inputFile}`);
    await fileSystemProtocol.get(inputFile, () => fs.createWriteStream(`${intermediateLocation}/${inputFile}`));
    // start maple worker
    console.log(`Maple: Processing ${inputFile}`);
    let keys = await runMaple(
      mapleScript,
      fs.createReadStream(`${intermediateLocation}/${inputFile}`),
      key => { let s = fs.createWriteStream(`${intermediateLocation}/${key}`); writes.push(streamToPromise(s)); return s; });
    // wait for intermediate results to be written to disk
    await Promise.all(writes);
    // upload results
    console.log(`Maple: Uploading results`);
    for (let i = 0; i < keys.length; i++) {
      // get lock from master
      await masterRequest('lock', { key: keys[i] });
      // perform append
      await fileSystemProtocol.append(`${task.intermediatePrefix}_${keys[i]}`,
        () => fs.createReadStream(`${intermediateLocation}/${keys[i]}`));
      // release lock from master
      await masterRequest('unlock', { key: keys[i] });
    }
    // append key set
    await masterRequest('lock', { key: `KeySet_${task.scriptName}` });

    await masterRequest('unlock', { key: `KeySet_${task.scriptName}` });
    // signal master that we're done
  });

  workerApp.post('/juiceTask', (req, res) => {
    if (!req.body) return res.sendStatus(400);
    if (currentTask) {
      return res.status(403).send('Task ${current.id} in progress');
    }
  });

  await Bluebird.promisify((x: number, cb) => workerApp.listen(x, cb))(WorkerPort);

  /**
   * Queue of jobs which have yet to complete.
   * When a job is done, it is removed from the head of the array.
   */
  let commandQueue = new ReactiveQueue<Command>();

  /**
   * The pool of tasks for the current command.
   */
  let taskPool = new Map<string, Task>();

  let ourId = ipToID(swim.whoami());
  // start master program if we are the leader
  leaderStream.subscribe(id => {
    if (id === ourId) {
      startMapleJuiceMaster();
    } else {
      stopMapleJuiceMaster();
    }
  });

  ///
  /// master server
  ///

  const masterApp = express();
  let server: http.Server = null;
  masterApp.use(bodyParser.json());

  masterApp.post('/maple', (req, res) => {
    if (!req.body) return res.sendStatus(400);
    let {
      mapleScriptName,
      numMaples,
      datasetPrefix,
      intermediatePrefix
    } = req.body;
    // create a new job
    let job: MapleJob = {
      type: 'maple',
      id: makeid(),
      taskIds: [],
      scriptName: mapleScriptName,
      numWorkers: numMaples,
      datasetPrefix,
      intermediatePrefix
    };
    // generate tasks
    for (let i = 0; i < numMaples; i++) {
      let task: MapleTask = {
        type: 'mapletask',
        id: makeid(),
        intermediatePrefix: intermediatePrefix,
        scriptName: mapleScriptName,
        jobId: job.id,
        inputFile: `M${datasetPrefix}_${mapleScriptName}_DS${i}`,
        state: 'waiting'
      };
      job.taskIds.push(task.id);
      taskPool.set(task.id, task);
    }
    commandQueue.push(job);
  });

  masterApp.post('/juice', (req, res) => {
    if (!req.body) return res.sendStatus(400);
  });

  let locks: { [x: string]: Semaphore } = {};

  masterApp.post('/lock', async (req, res) => {
    if (!req.body) return res.sendStatus(400);
    let key: string = req.body.key;
    if (!locks[key]) {
      // initialize
      locks[key] = new Semaphore();
    }
    // try locking existing semaphore
    await locks[key].acquire();
    res.sendStatus(200);
  });

  masterApp.post('/unlock', (req, res) => {
    if (!req.body) return res.sendStatus(400);
    let key: string = req.body.key;
    if (!locks[key]) {
      // initialize
      locks[key] = new Semaphore();
    } else {
      locks[key].release();
    }
    res.sendStatus(200);
  });

  function startMapleJuiceMaster() {
    if (!server) {
      server = masterApp.listen(MasterPort);
      console.log('Start MapleJuice Master');
    }
  }

  function stopMapleJuiceMaster() {
    if (server) {
      console.log('Stop MapleJuice Master');
      server.close();
      server = null;
    }
  }

  function workerRequest(api: string, id: number, body?: Task | WorkerQuery) {
    return rp({
      uri: `http://fa16-cs425-g06-${ id < 10 ? '0' + id : id }.cs.illinois.edu:${WorkerPort}/${api}`,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: body || {},
      json: true,
      encoding: 'utf8',
      gzip: false
    });
  }

  // scheduling logic
  commandQueue.subscribe(async job => {
    // find workers
    let members = swim.members().map(x => ipToID(x.host));
    members = _.shuffle(members);
    // find tasks
    let pendingTasks: Task[] = [];
    for (let key of taskPool.keys()) {
      let task = taskPool.get(key);
      if (task.state === 'waiting') {
        pendingTasks.push(task);
      }
    }
    pendingTasks = _.shuffle(pendingTasks);
    // assign tasks to workers
    let assignedTasks: Task[] = [];
    for (let i = 0; i < members.length; i++) {
      pendingTasks[i].assignedWorker = members[i];
      pendingTasks[i].state = 'progress';
      assignedTasks.push(pendingTasks[i]);
    }
    // send tasks
    await Promise.all(assignedTasks.map(task => workerRequest(
      task.type === 'mapletask' ? 'mapleJob' : 'juiceJob',
      task.assignedWorker,
      task)));
    // start querying worker states
    console.log('Job ${job.scriptName} done');
  });

  ///
  /// master client
  ///

  let haveLeader = Bluebird.defer();
  leaderStream.take(1).do(() => haveLeader.resolve()).subscribe();

  async function masterRequest(api: string, body?: MasterMaple | MasterJuice | MasterQuery | MasterLock) {
    await haveLeader.promise;
    let id = paxos().valueOr(1);
    return await rp({
      uri: `http://fa16-cs425-g06-${ id < 10 ? '0' + id : id }.cs.illinois.edu:${MasterPort}/${api}`,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: body || {},
      json: true,
      encoding: 'utf8',
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
    console.log(`Uploading Maple script ${mapleScriptName}`);
    await fileSystemProtocol.put(mapleScriptName, () => fs.createReadStream(mapleExe));
    // split and upload dataset
    let dataStreams = partitionDataset(sourceDirectory, numMaples);
    let datasetPrefix = Math.round(Math.random() * 1000000);
    console.log(`Uploading dataset`);
    await Promise.all(dataStreams.map((stream, index) =>
      fileSystemProtocol.put(`M${datasetPrefix}_${mapleScriptName}_DS${index}`, () => {
        // if error occurred during upload (this is invoked a second time)
        if ((<any> stream).usedDataset) {
          // redo everything
          dataStreams = partitionDataset(sourceDirectory, numMaples);
          return dataStreams[index];
        }
        (<any> stream).usedDataset = true;
        return stream;
      })));
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
