/// <reference path="../../typings/globals/mocha/index.d.ts" />
/// <reference path="../../typings/globals/chai/index.d.ts" />

import Semaphore from '../semaphore';
import * as Bluebird from 'bluebird';
import * as chai from 'chai';
const expect = chai.expect;

describe('Semaphore', function () {
  this.timeout(500);
  it('acquires correctly', async function () {
    let semaphore = new Semaphore();
    await semaphore.acquire();
    expect(1).to.be.eq(1);
  });

  it('blocks until released', async function () {
    let semaphore = new Semaphore();
    await semaphore.acquire();
    let timedout = await Promise.race([
      semaphore.acquire().then(() => false),
      Bluebird.delay(20).then(() => true)
    ]);
    expect(timedout).to.be.eq(true);
  })
});
