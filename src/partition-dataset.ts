import * as stream from 'stream';
import * as fs from 'fs';
import * as Bluebird from 'bluebird';
import * as path from 'path';

const readdir = Bluebird.promisify(fs.readdir);
const stat = Bluebird.promisify(fs.stat);

export function partitionDataset(dataset: string, numPartition: number): stream.Readable[] {
  // get all files in dataset along with their size
  let allFiles = readdir(dataset)
  .then(files => Promise.all(files.map(file => path.join(dataset, file)).map(file =>
    stat(file).then(st => <[string, fs.Stats]> [file, st]))))
  .then(filestats => filestats
    .filter(([_, stat]) => stat.isFile())
    .map(([file, stat]) => <[string, number]> [ file, stat.size ]));

  class PartitionedStream extends stream.Readable {

    readyHandlers: Function[];

    constructor(options?: any) {
      super(options);
      this.readyHandlers = [];
    }

    private fireHandlers() {
      this.readyHandlers.forEach(cb => cb());
      this.readyHandlers = [];
    }

    onReady(cb: Function) {
      this.readyHandlers.push(cb);
    }

    enqueue(...chunks: Buffer[]) {
      let ready = true;
      chunks.forEach(chunk => ready = ready && this.push(chunk));
      if (ready) {
        this.fireHandlers();
      }
    }

    _read(size: number) {
      this.fireHandlers();
    }

  }
  const streams: PartitionedStream[] = Array.apply(null, Array(numPartition)).map(() => new PartitionedStream());

  // get a rough partition of the files
  // actual partition may vary due to the exact position of new lines
  let roughPartition = allFiles
  .then(filestats => {
    let sizes = filestats.map(([_, size]) => size);
    let totalSize = sizes.reduce((a, b) => a + b, 0);
    let individualSize = Math.round(totalSize / numPartition);
    let fileStream: fs.ReadStream[] = [];
    if (filestats.length > 0) {
      fileStream.push(fs.createReadStream(filestats[0][0]));
    } else {
      return 0;
    }
    
    // pipe everything sequentially into our writable stream
    let masterStream = new MasterStream();
    let cb = idx => () => {
      if (idx < filestats.length - 1) {
        // push next file
        let nextIdx = idx + 1;
        console.log(`reading: ${filestats[nextIdx][0]}`);
        fileStream.push(fs.createReadStream(filestats[nextIdx][0]));
        fileStream[nextIdx].on('end', cb(nextIdx));
        // separate files with newline
        masterStream.write(new Buffer('\n'), () =>
          // pipe next file
          fileStream[nextIdx].pipe(masterStream, { end: nextIdx === filestats.length - 1 }));
      }
    };
    fileStream[0].on('end', cb(0));
    fileStream[0].pipe(masterStream, { end: filestats.length === 1 });

    // terminate all streams after the masterStream is clear
    masterStream.on('finish', () =>
      process.nextTick(() =>
        streams.forEach(stream =>
          stream.push(null))));

    return individualSize;
  });

  const lookAheadSize = 500;
  const newlineChar = '\n'.charCodeAt(0);
  class MasterStream extends stream.Writable {

    bytesReceived: number;
    currParition: number;

    constructor(options?: any) {
      super(options);
      this.bytesReceived = 0;
      this.currParition = 0;
    }

    _write(chunk: Buffer, encoding: string, callback: Function): void {
      // main partitioning logic
      roughPartition.then(individualSize => {
        let stream = streams[this.currParition];
        if (this.currParition === numPartition - 1) {
          // we're at last partition, just send whatever we have
          stream.onReady(callback);
          stream.enqueue(chunk);
        } else if (this.bytesReceived > (this.currParition + 1) * individualSize - lookAheadSize) {
          // need to scan for new line and terminate partition
          let newlineIndex = chunk.findIndex(v => v === newlineChar);
          if (newlineIndex !== -1) {
            // found newline character, terminate current stream just before newline
            let firstStreamReady = Bluebird.defer();
            let secondStreamReady = Bluebird.defer();
            stream.onReady(() => firstStreamReady.resolve());
            stream.enqueue(chunk.slice(0, newlineIndex + 1), null);
            // send rest to next stream
            let nextStream = streams[this.currParition + 1];
            nextStream.onReady(() => secondStreamReady.resolve());
            nextStream.enqueue(chunk.slice(newlineIndex + 1));
            // current write finishes when both stream acknowledges
            Bluebird.all([firstStreamReady.promise, secondStreamReady.promise]).then(() => callback());
            this.currParition += 1;
          } else {
            // didn't find any newline, need to continue
            stream.onReady(callback);
            stream.enqueue(chunk);
          }
        } else {
          stream.onReady(callback);
          stream.enqueue(chunk);
        }
        // console.log(`${this.bytesReceived} + ${chunk.length} = ${this.bytesReceived + chunk.length}`);
        this.bytesReceived += chunk.length;
      });
    }
  }

  return streams;
}
