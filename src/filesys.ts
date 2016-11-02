import * as express from 'express';
import swimFuture from './swim';
import * as Swim from 'swim';
import * as rp from 'request-promise';
import * as Bluebird from 'bluebird';

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

    let store = () => Promise.resolve(Object.keys(files));

    return await (<(port: number) => Bluebird<{}>> Bluebird.promisify(app.listen, { context: app }))(22895)
    .then(() => Object.freeze({
      put, get, del, ls, store
    }));
  });
