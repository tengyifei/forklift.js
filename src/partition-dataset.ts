import * as stream from 'stream';
import * as fs from 'fs';
import * as Bluebird from 'bluebird';
import * as path from 'path';

const readdir = Bluebird.promisify(fs.readdir);
const stat = Bluebird.promisify(fs.stat);

export function partitionDataset(dataset: string, numPartition: number): stream.Readable[] {
  // get all files in dataset along with their size
  let allFiles = readdir(dataset)
  .then(files => Promise.all(files.map(file =>
    stat(file).then(st => <[string, fs.Stats]> [path.join(dataset, file), st]))))
  .then(filestats => filestats
    .filter(([_, stat]) => stat.isFile())
    .map(([file, stat]) => <[string, number]> [ file, stat.size ]));

  class PartitionedStream extends stream.Readable {

    readyHandlers: Function[];

    constructor(options) {
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
  let streams = Array(numPartition).map((_, idx) => new PartitionedStream(idx));

  // get a rough partition of the files
  // actual partition may vary due to the exact position of new lines
  let roughPartition = allFiles
  .then(filestats => {
    let sizes = filestats.map((_, size) => size);
    let totalSize = sizes.reduce((a, b) => a + b, 0);
    let individualSize = Math.round(totalSize / numPartition);
    let fileStream: fs.ReadStream[] = [];
    filestats.forEach(([file, _]) => fileStream.push(fs.createReadStream(file)));
    
    // pipe everything sequentially into our writable stream
    let masterStream = new MasterStream();
    fileStream[0].pipe(masterStream, { end: false });
    for (let i = 0; i < filestats.length - 1; i++) {
      fileStream[i].on('end', (idx => () =>
        // separate files with newline
        masterStream.write(new Buffer('\n'), () =>
          // pipe next file
          fileStream[idx + 1].pipe(masterStream, { end: false }))
      )(i));
    }

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
    currBytesIntoPartition: number;

    constructor(options?: any) {
      super(options);
      this.bytesReceived = 0;
      this.currParition = 0;
      this.currBytesIntoPartition = 0;
    }

    _write(chunk: Buffer, encoding: string, callback: Function): void {
      // main partitioning logic
      roughPartition.then(individualSize => {
        let stream = streams[this.currParition];
        if (this.currParition === numPartition - 1) {
          // we're at last partition, just send whatever we have
          stream.onReady(callback);
          stream.enqueue(chunk);
        } else if (this.bytesReceived > this.currParition * individualSize - lookAheadSize) {
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
        this.bytesReceived += chunk.length;
      });
    }
  }

  return streams;
}
