import * as express from 'express';
import swimFuture from './swim';
import * as Swim from 'swim';
import * as rp from 'request-promise';
import * as Bluebird from 'bluebird';
import * as crypto from 'crypto';
const modexp = require('mod-exp');

export const fileSystemProtocol = swimFuture.then(async swim => {
    interface Dictionary {
      [key: string]: Buffer;
    }
    const app = express();
    const files: Dictionary = {};

    // send our file if possible
    app.post('/download', (req, res) => {
      let key = req.header('sdfs-key');
      if (key && files[key]) {
        res.send(files[key]);
      } else {
        res.sendStatus(404).send('Not found: ' + key);
      }
    });

    // receives binary and stores it as a buffer
    app.post('/upload', (req, res) => {
      let key = req.header('sdfs-key');
      if (key) {
        files[key] = new Buffer(req.body);
      } else {
        res.sendStatus(400).send('Must specify sdfs-key');
      }
    });

    // return if the file is present
    app.post('/query', (req, res) => {
      let key = req.header('sdfs-key');
      if (key) {
        res.send({ present: !!files[key] });
      } else {
        res.sendStatus(400).send('Must specify sdfs-key');
      }
    });

    // removes the file from memory
    app.post('/delete', (req, res) => {
      let key = req.header('sdfs-key');
      if (key) {
        res.send({ deleted: !!files[key] });
        delete files[key];
      } else {
        res.sendStatus(400).send('Must specify sdfs-key');
      }
    });

    let put = (key: string, file: Buffer) => new Promise((resolve, reject) => {

    });

    let get = (key: string) => new Promise((resolve, reject) => {

    });

    let del = (key: string) => new Promise((resolve, reject) => {

    });

    let ls = (key: string) => new Promise((resolve, reject) => {

    });

    // probing strategy for a node
    const hashKey = (key: string) => function*() {
      const md5sum = crypto.createHash('md5');
      md5sum.update(key);
      let digestHex = md5sum.digest('hex');
      // squash to 16-bit number
      let exponent = 0;
      for (let i = 0; i < 32; i += 4) {
        let part = +digestHex.substr(i, 4);
        exponent = exponent ^ part;
      }
      // generate probes
      for (let i = 0; i < 10; i++) {
        // (7 ^ (exponent + i)) mod 10
        yield <number> modexp(7, exponent + i, 10);
      }
    };

    // replicate when nodes fail

    let store = () => Promise.resolve(Object.keys(files));

    return await (<(port: number) => Bluebird<{}>> Bluebird.promisify(app.listen, { context: app }))(22895)
    .then(() => Object.freeze({
      put, get, del, ls, store
    }));
  });
