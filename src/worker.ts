import * as lineReader from 'line-reader';
import * as stream from 'stream';
import * as fs from 'fs';
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
  line: string;
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
    function run(msg: MasterMessage) {
      if (msg.type === 'line') {
        let kvs = mapper(msg.line);
        postMessage({ type: 'kvs', kvs }, '*');
      } else {
        postMessage({ type: 'dack' }, '*');
        self.close();
      }
    }
    // computation starter
    this.onmessage = event => run(event.data);
  });

  worker.onmessage = event => {
    let msg: WorkerMessage = event.data;
    if (msg.type === 'dack') {
      // worker has terminated
      kvFiles.forEach(stream => stream.end());
      handle.resolve(Array.from(kvFiles.keys()));
    } else {
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
    }
  }

  // inject maple program
  worker.thread.eval(mapleScript);

  // start the computation
  (<(x: stream.Readable, y: (z: string) => void) => Promise<void>> <any>
    Bluebird.promisify(lineReader.eachLine))(data, line => {
      // TODO: batching
      if (line.length !== 0) worker.postMessage({ type: 'line', line });
  })
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
