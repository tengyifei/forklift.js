import { fileSystemProtocol } from './filesys';
import swimFuture from './swim';
import { ipToID, MemberState } from './swim';
import { paxos, leaderStream } from './paxos';
import { Observable, ReplaySubject, Subject } from 'rxjs/Rx';
import { partitionDataset } from './partition-dataset';
import { ReactiveQueue } from './producer-consumer';
import { makeid, BufferingStream, hashString } from './utils';
import { maple as runMaple, juice as runJuice } from './worker';
import Semaphore from './semaphore';
import { Maybe } from './maybe';
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
import * as msgpack from 'msgpack-lite';
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
  numTaskDone: number;
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
  numTaskDone: number;
  numWorkers: number;
  scriptName: string;
  destFilename: string;
}

type Command = MapleJob | JuiceJob;

interface TaskBase {
  type: 'mapletask' | 'juicetask';
  jobId: string;
  id: string;
  scriptName: string;
  assignedWorker?: number;
  state: 'waiting' | 'progress' | 'done';
}

interface MapleTask extends TaskBase {
  type: 'mapletask';
  intermediatePrefix: string;
  inputFile: string;
}

interface JuiceTask extends TaskBase {
  type: 'juicetask';
  intermediatePrefix: string;
  inputKeys: string[];
  destFilename: string;
}

type Task = MapleTask | JuiceTask;

interface WorkerQuery {

}

const WorkerPort = 54321;
const MasterPort = 54777;

const intermediateLocation = 'mp_tmp';
const InProgressErrorCode = 499;

function sanitizeForFile(name: string): string {
  name = encodeURIComponent(name);
  name = name.split('').map(x => {
    if (x === '.'
     || x === '$'
     || x === '/'
     || x === ':'
     || x === '\\'
     || x === '?'
     || x === '*') {
      let charCode = x.charCodeAt(0);
      let str;
      if (charCode > 1000)
        str = '' + charCode;
      else if (charCode > 100)
        str = '0' + charCode;
      else if (charCode > 10)
        str = '00' + charCode;
      else
        str = '000' + charCode;
      return '$' + str;
    } else {
      return x;
    }
  }).join('');
  return name;
}

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
  let historicalTasks = new Map<string, Task>();

  // recreate tmp folder
  await Bluebird.promisify(rimraf)(intermediateLocation);
  await Bluebird.promisify(mkdirp)(intermediateLocation);

  const workerApp = express();
  workerApp.use(bodyParser.json());

  workerApp.post('/mapleTask', async (req, res) => {
    if (!req.body) return res.sendStatus(400);
    let task: Task = req.body;
    if (currentTask) {
      return res.status(InProgressErrorCode).send({ error: `Task ${currentTask.id} in progress` });
    }
    if (task.type !== 'mapletask') {
      return res.status(400).set({ error: 'Not a Maple task' });
    }
    // start
    currentTask = task;
    historicalTasks.set(task.id, task);
    let startTime = Date.now();
    res.status(200).send({ result: 'Working' });
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
      key => {
        let s = fs.createWriteStream(`${intermediateLocation}/${sanitizeForFile(key)}`);
        writes.push(streamToPromise(s));
        return s;
      });
    // wait for intermediate results to be written to disk
    await Promise.all(writes);
    let workerDoneTime = Date.now();
    // upload results
    console.log(`Maple: Uploading results`);
    for (let i = 0; i < keys.length; i++) {
      let key = keys[i];
      // get lock from master
      await masterRequest('lock', { key });
      // perform append
      await fileSystemProtocol.append(`${task.intermediatePrefix}_${key}`,
        () => fs.createReadStream(`${intermediateLocation}/${sanitizeForFile(key)}`));
      // release lock from master
      await masterRequest('unlock', { key });
    }
    // append key set
    await masterRequest('lock', { key: `${task.intermediatePrefix}_KeySet` });
    // write key set
    let encodedKeys = msgpack.encode(keys);
    await fileSystemProtocol.append(`${task.intermediatePrefix}_KeySet`,
      () => {
        let s = new stream.Readable();
        s.push(encodedKeys);
        s.push(null);
        return s;
      });
    await masterRequest('unlock', { key: `${task.intermediatePrefix}_KeySet` });
    let endTime = Date.now();
    console.log(`Maple: Done in ${
      (endTime - startTime) / 1000} seconds. Processing time was ${
      (workerDoneTime - startTime) / 1000} seconds`);
    // signal master that we're done
    await masterRequest('taskDone', task);
    currentTask = null;
  });

  workerApp.post('/juiceTask', async (req, res) => {
    if (!req.body) return res.sendStatus(400);
    let task: Task = req.body;
    if (currentTask) {
      return res.status(403).send({ error: 'Task ${current.id} in progress' });
    }
    if (task.type !== 'juicetask') {
      return res.status(400).send({ error: 'Not a Maple task' });
    }
    // start
    currentTask = task;
    historicalTasks.set(task.id, task);
    let startTime = Date.now();
    res.status(200).send({ result: 'Working' });
    // download maple script
    console.log(`Juice: Downloading script ${task.scriptName}`);
    let juiceScript: string;
    await fileSystemProtocol.get(task.scriptName, () => {
      let s = new BufferingStream();
      s.on('finish', () => { juiceScript = s.result.toString(); });
      return s;
    });
    // download our part of the keys
    let keySet = task.inputKeys;
    console.log(`Juice: Downloading keys`);
    await Bluebird.map(
      keySet,
      key => fileSystemProtocol.get(`${task.intermediatePrefix}_${key}`, () => fs.createWriteStream(`${intermediateLocation}/${sanitizeForFile(key)}`)),
      { concurrency: 3 });
    // start juice worker
    console.log(`Juice: Processing`);
    let keys = await runJuice(
      juiceScript,
      task.inputKeys,
      key => fs.createReadStream(`${intermediateLocation}/${sanitizeForFile(key)}`),
      fs.createWriteStream(`${intermediateLocation}/ReducerOutput_${task.scriptName}`));
    let workerDoneTime = Date.now();
    // upload results
    console.log(`Juice: Uploading results`);
    await masterRequest('lock', { key: `${task.destFilename}` });
    await fileSystemProtocol.append(`${task.destFilename}`,
      () => fs.createReadStream(`${intermediateLocation}/ReducerOutput_${task.scriptName}`));
    await masterRequest('unlock', { key: `${task.destFilename}` });
    let endTime = Date.now();
    console.log(`Juice: Done in ${
      (endTime - startTime) / 1000} seconds. Processing time was ${
      (workerDoneTime - startTime) / 1000} seconds`);
    // signal master that we're done
    await masterRequest('taskDone', task);
    currentTask = null;
  });

  workerApp.post('/taskStatus', (req, res) => {
    if (!req.body) return res.sendStatus(400);
    res.status(200).send(historicalTasks.get(req.body.taskId));
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

  let taskDoneEvents = new ReplaySubject<Task>();
  let workerAvailableEvents = new Subject<number>();
  let taskAvailableEvents = new Subject<Task>();

  let ourId = ipToID(swim.whoami());

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
      numTaskDone: 0,
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

    res.sendStatus(200);
  });

  masterApp.post('/juice', async (req, res) => {
    if (!req.body) return res.sendStatus(400);
    let {
      juiceScriptName,
      numJuices,
      intermediatePrefix,
      destFilename,
      deleteInput,
      partitionAlgorithm
    } = req.body;
    // download key set
    let keySetName = `${intermediatePrefix}_KeySet`;
    console.log(`Downloading key set ${keySetName}`);
    // split keys into numJuices parts
    let receiveKeySet: BufferingStream;
    await fileSystemProtocol.get(keySetName, () => receiveKeySet = new BufferingStream());
    let keySet: string[] = msgpack.decode(receiveKeySet.result);
    let partitionKeySet: string[][] = [];
    for (let i = 0; i < numJuices; i++) {
      partitionKeySet.push([]);
    }
    if (partitionAlgorithm === 'range') {
      keySet = keySet.sort();
      let numKeys = keySet.length;
      let numPerPartition = Math.ceil(numKeys / numJuices);
      for (let i = 0; i < numJuices; i++) {
        for (let j = 0; j < numPerPartition; j++) {
          let index = i * numPerPartition + j;
          if (index >= numKeys) break;
          partitionKeySet[i].push(keySet[index]);
        }
      }
    } else if (partitionAlgorithm === 'hash') {
      keySet.forEach(key => partitionKeySet[hashString(key) % numJuices].push(key));
    } else {
      console.warn(`Bad partition algorithm ${partitionAlgorithm}`);
      res.sendStatus(400);
      return;
    }
    // create a new job
    let job: JuiceJob = {
      type: 'juice',
      id: makeid(),
      taskIds: [],
      numTaskDone: 0,
      numWorkers: numJuices,
      scriptName: juiceScriptName,
      destFilename: destFilename
    };
    // generate tasks
    for (let i = 0; i < numJuices; i++) {
      let task: JuiceTask = {
        type: 'juicetask',
        id: makeid(),
        intermediatePrefix: intermediatePrefix,
        scriptName: juiceScriptName,
        destFilename: destFilename,
        jobId: job.id,
        inputKeys: partitionKeySet[i],
        state: 'waiting'
      };
      job.taskIds.push(task.id);
      taskPool.set(task.id, task);
    }
    commandQueue.push(job);

    res.sendStatus(200);
  });

  masterApp.post('/taskDone', (req, res) => {
    if (!req.body) return res.sendStatus(400);
    taskDoneEvents.next(req.body);
    res.sendStatus(200);
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
    res.status(200).send({ state: 'locked' });
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
    res.status(200).send({ state: 'unlocked' });
  });

  const startMapleJuiceMaster = () => {
    if (!server) {
      server = masterApp.listen(MasterPort);
      console.log('Start MapleJuice Master');
    }
  }

  const stopMapleJuiceMaster = () => {
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
    let activeWorkers = new Set<number>();
    let assigned = new ReplaySubject<number>();

    // scheduling logic
    let scheduleQueue = new ReactiveQueue<{ type: 'worker', worker: number} | { type: 'task', task: Task }>();
    let subX = Observable.merge(
      workerAvailableEvents.map(w => ({ type: 'worker', worker: w })),
      taskAvailableEvents.map(t => ({ type: 'task', task: t})))
    .subscribe(x => scheduleQueue.push(<any> x));

    scheduleQueue.subscribe(async wt => {
      let waitingTask: Task = null;
      let freeWorker: number = NaN;
      if (wt.type === 'task') {
        waitingTask = wt.task;
        // find first available worker
        let members = swim.members().map(x => ipToID(x.host));
        members = _.shuffle(members);
        for (let i = 0; i < members.length; i++) {
          if (activeWorkers.has(members[i]) === false) {
            freeWorker = members[i];
            break;
          }
        }
      } else {
        freeWorker = wt.worker;
        for (let i = 0; i < job.taskIds.length; i++) {
          // find first waiting task
          if (taskPool.has(job.taskIds[i])
            && taskPool.get(job.taskIds[i]).state === 'waiting') {
              waitingTask = taskPool.get(job.taskIds[i]);
              break;
            }
        }
      }
      // try to schedule
      if (waitingTask && isFinite(freeWorker)) {
        waitingTask.assignedWorker = freeWorker;
        console.log(`Task ${waitingTask.id} -> Worker ${freeWorker}`);
        await workerRequest(
          waitingTask.type === 'mapletask' ? 'mapleTask' : 'juiceTask',
          waitingTask.assignedWorker,
          waitingTask)
        .then(_ => {
          waitingTask.state = 'progress';
          activeWorkers.add(freeWorker);
          assigned.next(0);
        })
        .catch(err => {
          waitingTask.assignedWorker = NaN;
          if (err.statusCode === InProgressErrorCode) {
            // we mistakenly thought that this worker was free
            activeWorkers.add(freeWorker);
          }
          console.error(err.error ? err.error : err);
          // retry in a while
          Bluebird.delay(500).then(() => taskAvailableEvents.next(waitingTask));
        });
      }
    });

    // track task state
    let jobDone = Bluebird.defer();
    let sub = taskDoneEvents
    .distinctUntilKeyChanged('id')    // one event per task
    .subscribe(task => {
      if (taskPool.has(task.id)) {
        // mark as done
        if (job.id === task.jobId) {
          if (taskPool.get(task.id).state !== 'done') {
            taskPool.get(task.id).state = 'done';
            job.numTaskDone += 1;
          }
          if (job.numTaskDone === job.taskIds.length) {
            // the job is done
            taskPool.clear();
            jobDone.resolve();
            // reset the task stream
            taskDoneEvents.complete();
            taskDoneEvents = new ReplaySubject<Task>();
            scheduleQueue.reset();
            workerAvailableEvents = new Subject<number>();
            taskAvailableEvents = new Subject<Task>();
            activeWorkers = new Set<number>();
            sub.unsubscribe();
            subX.unsubscribe();
          } else {
            // free this worker
            activeWorkers.delete(task.assignedWorker);
            // see if we can schedule more tasks
            workerAvailableEvents.next(task.assignedWorker);
          }
        } else {
          console.warn(`Task ${task.id} does not belong to job ${job.id}`);
        }
      } else {
        console.warn(`Unknown task reported: ${task.id}`);
      }
    });

    // wait for initial scheduling
    pendingTasks.forEach(t => taskAvailableEvents.next(t));
    members.forEach(id => workerAvailableEvents.next(id));
    await assigned
    // wait for numWorker/numTask assignments, whichever is smaller
    .skip(Math.min(pendingTasks.length, members.length) - 1)
    .take(1).toPromise();

    // start querying worker states
    for (;;) {
      let queryOnce = async () => {
        // poll all current workers
        for (let [_, id] of activeWorkers.entries()) {
          for (let [taskId, task] of taskPool.entries()) {
            if (task.assignedWorker === id && task.state === 'progress') {
              let taskId = task.id;
              // if any fails, put the task back to waiting, so that it may be rescheduled
              let working = await workerRequest('taskStatus', id, { taskId: taskId })
              .then(res => res.id && taskPool.get(res.id).jobId === job.id
                          ? Maybe.just<Task>(res)
                          : Maybe.nothing<Task>())
              .catch(() => Maybe.nothing<Task>());
              working.caseOf({
                just: task => {
                  // we're good
                  if (task.state === 'done') {
                    taskDoneEvents.next(task);
                  }
                },
                nothing: () => {
                  console.warn(`Worker ${id} is down. Rescheduling.`);
                  activeWorkers.delete(id);
                  for (let [taskId, task] of taskPool.entries()) {
                    if (task.assignedWorker === id && task.state === 'progress') {
                      task.state = 'waiting';
                      task.assignedWorker = NaN;
                      taskAvailableEvents.next(task);
                    }
                  }
                }
              });
            }
          }
        }
      };
      let done = await Promise.race([
        jobDone.promise.then(() => true),
        Promise.race([
          queryOnce().then(() => Bluebird.delay(1000)),
          Bluebird.delay(1000)
        ]).then(() => false)
      ]);
      if (done) break;
    }
    console.log(`Job ${job.scriptName} done`);
  });

  swim.on(Swim.EventType.Change, update => {
    if (update.state === MemberState.Alive) {
      // leave time for initialization
      setTimeout(() => workerAvailableEvents.next(ipToID(update.host)), 1500);
    }
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
    let numConcurrentStreams = 1;
    let concurrentStreams = [ <[stream.Readable[], number]> [dataStreams, 0]];
    for (let i = 0; i < dataStreams.length; i++) {
      await fileSystemProtocol.put(`M${datasetPrefix}_${mapleScriptName}_DS${i}`, (index => () => {
        // find a suitable stream array, or create a new one
        for (let x = 0; x < concurrentStreams.length; x++) {
          if (concurrentStreams[x][1] === index) {
            concurrentStreams[x][1] += 1;
            return concurrentStreams[x][0][index];
          }
        }
        // not found
        let newStream = partitionDataset(sourceDirectory, numMaples);
        // exhaust stream up to index - 1
        for (let x = 0; x <= index - 1; x++) {
          newStream[x].on('data', (chunk) => {});
        }
        concurrentStreams.push([newStream, index + 1]);
        return newStream[index];
      })(i));
    }
    // start job
    await masterRequest('maple', {
      mapleScriptName,
      numMaples,
      datasetPrefix,
      intermediatePrefix
    });
    console.log(`Maple job ${mapleScriptName} sent`);
  }

  async function juice(
    juiceExe: string,
    numJuices: number,
    intermediatePrefix: string,
    destFilename: string,
    deleteInput: boolean = false,
    partitionAlgorithm: 'hash' | 'range' = 'hash')
  {
    let leader = await withLeader();
    // upload juice script
    let juiceScriptName = path.basename(juiceExe, '.js');
    console.log(`Uploading Juice script ${juiceScriptName}`);
    await fileSystemProtocol.put(juiceScriptName, () => fs.createReadStream(juiceExe));
    // start job
    await masterRequest('juice', {
      juiceScriptName,
      numJuices,
      intermediatePrefix,
      destFilename,
      deleteInput,
      partitionAlgorithm
    });
    console.log(`Juice job ${juiceScriptName} sent`);
  }

  // start master program if we are the leader
  leaderStream.subscribe(id => {
    if (id === ourId) {
      startMapleJuiceMaster();
    } else {
      stopMapleJuiceMaster();
    }
  });

  return { maple, juice };

});
