import * as lineReader from 'line-reader';
import * as stream from 'stream';
import * as fs from 'fs';
import * as es from 'event-stream';
import * as Bluebird from 'bluebird';
import * as msgpack from 'msgpack-lite';
const Worker = require('webworker-threads').Worker;

/*
var readStream = fs.createReadStream('test.log', { start: 0, end: 10000 });
lineReader.eachLine(readStream, function(line) {});
*/

type MapleFunction = (line: string) => [string, string][];

interface NewLine {
  type: 'line';
  lines: string[];
}
interface Done {
  type: 'done';
}
interface KVPairs {
  type: 'kvs',
  kvs: [string, string][];
}
interface DoneAck {
  type: 'dack';
}
type MasterMessage = NewLine | Done;
type WorkerMessage = KVPairs | DoneAck;

// mapper function which gets loaded by worker
declare var mapper: MapleFunction;

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
        let collateKv: { [x: string]: string[] } = {};
        kvs.forEach(kv => {
          let [key, value] = kv;
          // ignore empty keys
          if (key === '') return;
          // ignore keys that are too long
          if (key.length > 500) return;
          if (!collateKv[key]) collateKv[key] = [];
          collateKv[key].push(value);
        });
        kvs = [].concat(...Object.keys(collateKv).map(k => collateKv[k].map(v => <[string, string]> [k, v])));
        postMessage({ type: 'kvs', kvs }, '*');
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

  worker.onmessage = event => {
    let msg: WorkerMessage = event.data;
    if (msg.type === 'dack') {
      // worker has terminated
      kvFiles.forEach(stream => stream.end());
      handle.resolve(Array.from(kvFiles.keys()));
    } else {
      totalBatchesProcessed += 1;
      // write worker output to file
      msg.kvs.forEach(kv => {
        let [key, value] = kv;
        if (kvFiles.has(kv[0]) === false) {
          // create new file
          let output = outputs(key);
          let encodeStream = msgpack.createEncodeStream();
          encodeStream.pipe(output);
          kvFiles.set(key, encodeStream);
        }
        kvFiles.get(key).write(value);
      });
      if (totalBatchesRead - totalBatchesProcessed < 80) {
        backlogCallbacks.forEach(cb => cb());
        backlogCallbacks = [];
      }
    }
  }

  // inject maple program
  worker.thread.eval(mapleScript);

  let lineBatch = [];
  let watermark = 10000;
  let totalLines = 0;

  // start the computation
  let dataRead = Bluebird.defer();
  let dataStream = data
  .pipe((<any> es.split)())
  .pipe(es.map((line, cb) => {
    totalLines += 1;
    if (totalLines > watermark) {
      console.log(`Read ${watermark} lines`);
      watermark += 10000;
    }
    if (line.length !== 0) {
      lineBatch.push(line);
    }
    if (lineBatch.length > 500) {
      totalBatchesRead += 1;
      worker.postMessage({ type: 'line', lines: lineBatch });
      lineBatch = [];
    }
    if (totalBatchesRead - totalBatchesProcessed >= 80) {
      // stop reading
      backlogCallbacks.push(cb);
    } else {
      cb();
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

