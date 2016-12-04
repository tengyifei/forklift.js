/// <reference path="../../typings/globals/mocha/index.d.ts" />
/// <reference path="../../typings/globals/chai/index.d.ts" />

import { partitionDataset } from '../partition-dataset';
import * as Bluebird from 'bluebird';
import * as chai from 'chai';
import * as fs from 'fs';
import * as Stream from 'stream';
import * as msgpack from 'msgpack-lite';
const expect = chai.expect;

// module to be tested
import { maple } from '../worker';

describe('Worker', function () {
  it('handles maple', async function () {
    let testData =
`apple bla apple
apple
pinapple pinapple pinapple pinapple pinapple pinapple pinapple
pen
pen
apple pinapple pinapple pinapple pinapple pinapple pinapple
apple
pen
pen
apple
apple pinapple pinapple pinapple pinapple pinapple pinapple
pen
pen pinapple pinapple pinapple pinapple pinapple pinapple
apple pinapple pinapple pinapple pinapple pinapple pinapple
apple
bla`;
    let expectedKeysAfterMaple = { apple: true, bla: true, pinapple: true, pen: true };
    let expectedAfterReduce = {
      apple: 9,
      bla: 2,
      pinapple: 31,
      pen: 6
    };
    let script =
`
function mapper(line) {
  return line.split(' ')
  .map(word => word.replace(/^[^a-z\d]*|[^a-z\d]*$/gi, ''))  // trim symbols
  .map(word => word.toLowerCase())  // to lower case
  .map(word => [word, 1]);  // to key-value pair
}
`;
    let input = new Stream.Readable();
    input.push(testData);
    input.push(null);

    class TestStream extends Stream.Writable {
      public result: Buffer;
      constructor () {
        super();
        this.result = Buffer.from([]);
      }
      _write (chunk, enc, next) {
        this.result = Buffer.concat([this.result, chunk]);
        next();
      }
    }
    let outputs: { [x: string]: TestStream } = {};
    let keys = await maple(script, input, key => {
      let output = new TestStream();
      outputs[key] = output;
      return output;
    });

    expect(keys.length).to.equal(Object.keys(expectedKeysAfterMaple).length);
    keys.forEach(k => expect(expectedKeysAfterMaple[k]).to.be.true);

    // verify result
    return Promise.all(Object.keys(outputs).map(async k => {
      let msgs = new Stream.Readable();
      msgs.push(outputs[k].result);
      msgs.push(null);
      let decodeStream = msgpack.createDecodeStream();
      let values = [];
      msgs.pipe(decodeStream).on('data', x => values.push(x));

      let msgsDefer = Bluebird.defer<string[]>();
      msgs.on('end', () => msgsDefer.resolve(values));

      await msgsDefer.promise;

      expect(expectedAfterReduce[k]).to.be.greaterThan(0);
      expect(values.length).to.be.greaterThan(0);
      values.forEach(value => expect(value).to.be.eq(1, 'each item should be one'));
      expect(expectedAfterReduce[k]).to.be.eq(values.length);
    }));
  });
});
