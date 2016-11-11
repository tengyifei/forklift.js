/// <reference path="../../typings/globals/mocha/index.d.ts" />
/// <reference path="../../typings/globals/chai/index.d.ts" />

import { partitionDataset } from '../partition-dataset';
import * as rimraf from 'rimraf';
import * as mkdirp from 'mkdirp';
import * as Bluebird from 'bluebird';
import * as chai from 'chai';
import * as fs from 'fs';
import * as Stream from 'stream';
const streamToPromise: (x: Stream.Readable) => Promise<Buffer> = require('stream-to-promise');
const expect = chai.expect;

function makeid() {
  var text = "";
  var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  var len = 3 + Math.random() * 20;
  for (var i = 0; i < len; i++)
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  return text;
}

describe('Partition dataset', function () {

  const testFolder = 'dist/test-partition';
  this.timeout(10000000);

  let allLines: string[] = [];

  before(async () => {
    // teardown test dataset
    await Bluebird.promisify(rimraf)(testFolder);
    await Bluebird.promisify(mkdirp)(testFolder);
    // create some data
    for (let i = 100; i < 150; i++) {
      let words = [];
      let line = [];
      for (let j = 0; j < 50000; j++) {
        let randomWord = makeid();
        words.push(randomWord);
        line.push(randomWord);
        if (Math.random() < 0.09) {
          words.push('\n');
          allLines.push(line.join(' '));
          line = [];
        }
      }
      await (<(x: string, y: string) => Promise<void>> <any> Bluebird.promisify(fs.writeFile))
        (`${testFolder}/${i}`, words.join(' '));
    }
  });

  async function verifyPartition(streams: Stream.Readable[]) {
    let allDataArr = await Bluebird.all(streams.map(streamToPromise));
    let allData = Buffer.concat(allDataArr);
    let myLines = allData.toString().split('\n');
    expect(myLines).to.deep.equal(allLines);
  }

  it('partitions correctly (one)', function () {
    return verifyPartition(partitionDataset(testFolder, 1));
  });

  it('partitions correctly (two)', function () {
    return verifyPartition(partitionDataset(testFolder, 2));
  });

  it('partitions correctly (many)', function () {
    return verifyPartition(partitionDataset(testFolder, 300));
  });
});
