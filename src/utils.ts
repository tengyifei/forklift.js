import * as stream from 'stream';

export function makeid() {
  var text = "";
  var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  var len = 20;
  for (var i = 0; i < len; i++)
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  return text;
}

export class BufferingStream extends stream.Writable {
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