import * as lineReader from 'line-reader';
import * as stream from 'stream';
import * as fs from 'fs';
import * as es from 'event-stream';
import * as Bluebird from 'bluebird';
import * as msgpack from 'msgpack-lite';
const Worker = require('webworker-threads').Worker;
import * as through2 from 'through2';

type MapleFunction = (line: string) => [string, any][];
type EmitterFunction = (key: string, value: any) => void;
interface MyObservable {
  subscribe: (fn: (value: string) => void) => Promise<void>;
}
type JuiceFunction = (key: string, values: MyObservable, emit: EmitterFunction) => Promise<void>;

// main -> worker
interface NewKV {
  type: 'kv',
  kv: [string, any][];
}
interface NewLine {
  type: 'line';
  lines: string[];
}
interface Done {
  type: 'done';
}
interface SetKey {
  type: 'setkey',
  key: string;
}
interface Values {
  type: 'values',
  values: any[];
}

// worker -> main
interface KVPairs {
  type: 'kvs',
  kvs: [string, any[]][];
}
interface SingleKV {
  type: 'kv',
  key: string;
  value: any;
}
interface AckBatch {
  type: 'ackbatch';
  num: number;
}
interface DoneAck {
  type: 'dack';
}

type MasterMessage = SetKey | NewLine | Done | NewKV | Values;
type WorkerMessage = KVPairs | DoneAck | SingleKV | AckBatch;

// mapper function which gets loaded by worker
declare var mapper: MapleFunction;
declare var reducer: JuiceFunction;

/**
 * Output of maple function is an array of keys, and a set of [streams of values in array form]
 */
export function maple(mapleScript: string, data: stream.Readable, outputs: (key: string) => stream.Writable): Bluebird<string[]> {
  let handle = Bluebird.defer<string[]>();

  let kvFiles = new Map<string, msgpack.EncodeStream>();

  // evaluate mapleExe and get the maple function
  let worker = new Worker(function() {
    let totalLines = 0;
    let watermark = 10000;
    function run(msg: MasterMessage) {
      if (msg.type === 'line') {
        // process this batch
        totalLines += msg.lines.length;
        if (totalLines > watermark) {
          console.log(`Past ${watermark} lines`);
          watermark += 10000;
        }
        let kvs = (<[string, string][]>[]).concat(...msg.lines.map(mapper));
        let collateKv: { [x: string]: any[] } = {};
        kvs.forEach(kv => {
          let [key, value] = kv;
          // ignore empty keys
          if (key === '') return;
          // ignore keys that are too long
          if (key.length > 500) return;
          if (!collateKv[key]) collateKv[key] = [];
          collateKv[key].push(value);
        });
        let keyIndexed = [].concat(Object.keys(collateKv).map(k => [k, collateKv[k]]));
        postMessage({ type: 'kvs', kvs: keyIndexed }, '*');
      } else {
        postMessage({ type: 'dack' }, '*');
        self.close();
      }
    }
    // computation starter
    this.onmessage = event => run(event.data);
  });

  let totalBatchesProcessed = 0;
  let totalBatchesRead = 0;
  let backlogCallbacks = [];
  let dataStream;

  worker.onmessage = event => {
    let msg: WorkerMessage = event.data;
    if (msg.type === 'dack') {
      // worker has terminated
      kvFiles.forEach(stream => stream.end());
      handle.resolve(Array.from(kvFiles.keys()));
    } else if (msg.type === 'kvs') {
      totalBatchesProcessed += 1;
      // write worker output to file
      Promise.all(msg.kvs.map(async kv => {
        let [key, values] = kv;
        if (values.length === 0) return;
        if (kvFiles.has(key) === false) {
          // create new file
          let output = outputs(key);
          let encodeStream = msgpack.createEncodeStream();
          encodeStream.pipe(output);
          kvFiles.set(key, encodeStream);
        }
        let stream = kvFiles.get(key);
        for (let i = 0; i < values.length - 1; i++) {
          let value = values[i];
          stream.write(value);
        }
        // wait for last element
        return Bluebird.promisify((v, cb) => stream.write(v, () => cb()))(values[values.length - 1]);
      }))
      .then(_ => {
        // attempt to resume after all writes have been flushed
        if (totalBatchesRead - totalBatchesProcessed < 1) {
          // resume data stream
          backlogCallbacks.forEach(cb => cb());
          backlogCallbacks = [];
          data.resume();
        }
      });
    }
  }

  // inject maple program
  worker.thread.eval(mapleScript);

  let lineBatch = [];
  let watermark = 10000;
  let totalLines = 0;

  // start the computation
  let dataRead = Bluebird.defer();
  dataStream = data
  .pipe(<any> es.split('\n'))
  .pipe(es.map((line, cb) => {
    totalLines += 1;
    if (totalLines > watermark) {
      console.log(`Read ${watermark} lines`); 
      watermark += 10000;
    }
    if (totalBatchesRead - totalBatchesProcessed >= 2) {
      // pause reading
      backlogCallbacks.push(cb);
      data.pause();
    } else {
      cb();
    }
    if (line.length !== 0) {
      lineBatch.push(line);
    }
    if (lineBatch.length > 10) {
      totalBatchesRead += 1;
      worker.postMessage({ type: 'line', lines: lineBatch });
      lineBatch = [];
    }
  }));
  dataStream.on('end', () => dataRead.resolve());
  dataStream.on('error', err => dataRead.reject(err));

  dataRead.promise
  .then(() => worker.postMessage({ type: 'line', lines: lineBatch }))    // post remaining batch
  .then(() => Bluebird.delay(50))
  .then(() => worker.postMessage({ type: 'done' }))
  .catch(err => {
    worker.terminate();
    handle.reject(err);
    kvFiles.forEach(stream => stream.end());
  });

  return handle.promise;
}

/**
 * Output of juice function is plaintext in {key: value} format, separated by line
 */
export async function juice(juiceScript: string, keys: string[], inputStreamer: (k: string) => NodeJS.ReadableStream, destinationStream: stream.Writable) {

  let worker = new Worker(function() {
    let resolver, rejector;
    let reducerEnd: Promise<void>;
    let endPromise = new Promise((res, rej) => {
      resolver = res;
      rejector = rej;
    });
    /**
     * This function is called when a new value is to be sent to output.
     */
    function emitter(key: string, value: any) {
      postMessage(<SingleKV> { type: 'kv', key, value }, '*');
    }
    let subscribers: ((v: any) => void)[] = [];
    let values = {
      subscribe: (fn: (v: any) => void) => {
        subscribers.push(fn);
        return endPromise;
      }
    };
    let theKey;
    let valuesReadNoAck = 0;
    function run(msg: MasterMessage) {
      if (msg.type === 'setkey') {
        // initialize everything
        theKey = msg.key;
        valuesReadNoAck = 0;
        subscribers = [];
        endPromise = new Promise((res, rej) => {
          resolver = res;
          rejector = rej;
        });
        reducerEnd = reducer(theKey, values, emitter);
      } else if (msg.type === 'values') {
        valuesReadNoAck += msg.values.length;
        // ack values in batches
        if (valuesReadNoAck >= 100) {
          postMessage({ type: 'ackbatch', num: valuesReadNoAck }, '*');
          valuesReadNoAck = 0;
        }
        // send all of them to reducer
        msg.values.forEach(val => subscribers.forEach(sub => sub(val)));
      } else if (msg.type === 'done') {
        // kick-start end phase of reducer
        resolver();
        // wait for all values to be emitted
        reducerEnd.then(() => postMessage({ type: 'dack' }, '*'));
      } else {
        console.warn(`Unknown message`, msg);
      }
    }
    this.onmessage = event => run(event.data);
  });

  // inject juice program
  worker.thread.eval(juiceScript);
  let handles = {};
  keys.forEach(k => handles[k] = Bluebird.defer<void>());

  function processSingleKey(key: string, data: NodeJS.ReadableStream) {
    let totalValuesProcessed = 0;
    let totalValues = 0;
    let backlogCallbacks = [];
    let dataStream: NodeJS.ReadWriteStream;

    worker.onmessage = event => {
      let msg: WorkerMessage = event.data;
      if (msg.type === 'dack') {
        // worker is done with current key
        handles[key].resolve();
      } else if (msg.type === 'kv') {
        // write worker output to file
        Bluebird.promisify((v, cb) => destinationStream.write(v, () => cb()))(
          JSON.stringify({ key: msg.key, value: msg.value }) + '\n')
        .then(_ => {
          // attempt to resume after all writes have been flushed
          if (totalValues - totalValuesProcessed < 200) {
            // resume data stream
            backlogCallbacks.forEach(cb => cb());
            backlogCallbacks = [];
            console.log('resume');
            // data.resume();
          }
        });
      } else if (msg.type === 'ackbatch') {
        totalValuesProcessed += msg.num;
        console.log(totalValues, totalValuesProcessed);
        // attempt to resume after all writes have been flushed
        if (totalValues - totalValuesProcessed < 200) {
          // resume data stream
          backlogCallbacks.forEach(cb => cb());
          backlogCallbacks = [];
          console.log('resume');
          // data.resume();
        }
      } else {
        console.warn(`Unknown message`, msg);
      }
    }

    let valueBatch = [];
    let watermark = 10000;

    worker.postMessage({ type: 'setkey', key: key });

    // start the computation
    let dataRead = Bluebird.defer();
    dataStream = data
    .pipe(msgpack.createDecodeStream())
    .pipe(through2.obj((value, enc, cb) => {
      totalValues += 1;
      if (totalValues > watermark) {
        console.log(`Read ${watermark} values`);
        watermark += 10000;
      }
      valueBatch.push(value);
      if (valueBatch.length >= 100) {
        worker.postMessage({ type: 'values', values: valueBatch });
        valueBatch = [];
      }
      if (totalValues - totalValuesProcessed >= 600) {
        // pause reading
        // backlogCallbacks.push(cb);
        cb();
        console.log('pause');
        // data.pause();
      } else {
        cb();
      }
    }))
    .on('data', () => {});
    dataStream.on('end', () => dataRead.resolve());
    dataStream.on('error', err => dataRead.reject(err));

    return dataRead.promise
    .then(() => console.log(key))
    .then(() => worker.postMessage({ type: 'values', values: valueBatch }))    // post remaining batch
    .then(() => console.log(key))
    .then(() => worker.postMessage({ type: 'done' }))
    .then(() => console.log(key))
    .then(() => handles[key].promise)
    .then(() => console.log(key))
    .catch(err => {
      worker.terminate();
      handles[key].reject(err);
      destinationStream.end();
    });
  }

  for (let i = 0; i < keys.length; i++) {
    console.log(`Key: ${keys[i]}`);
    let key = keys[i];
    await processSingleKey(key, inputStreamer(key));
  }
  await Bluebird.delay(50);
  worker.terminate();
  destinationStream.end();
  await Bluebird.delay(50);
}
