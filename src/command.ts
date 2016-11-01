import * as readline from 'readline';
import * as tty from 'tty';
const charm = require('charm')(process);

export interface Command {
  kind: 'command';
  value: string;
}

export interface Exit {
  kind: 'exit';
}

export type Input = Command | Exit; 

export let runConsole = (processor: (x: Input) => void) => {
  // clears screen
  charm.reset();
  const stdin = process.stdin;
  (<tty.ReadStream> stdin).setRawMode(true);

  // cache the command
  let input = '';

  // command input
  console.log('SDFS> ');
  let outputx = 0, outputy = 2;

  let inputx = 6;

  stdin.on('data', key => {
    if (key.toString() === '\r') {
      // process input
      let inputCopy = input.split('').join('');
      if (inputCopy !== '') {
        setTimeout(() => processor({ kind: 'command', value: inputCopy }), 1);
      }
      // enter key
      input = '';
      inputx = 6;
      // clear command
      charm.position(0, 0);
      charm.erase('line');
      charm.write('SDFS> ');
    } else if (key.toString() === '\u007f') {
      // backspace
      if (inputx > 6) {
        charm.position(inputx, 0);
        charm.erase('end');
        input = input.substr(0, input.length - 1);
        inputx -= 1;
      } else {
        charm.position(7, 0);
      }
    } else {
      // hack for charm problem. filter out undisplayable characters
      if (/^([!@#$%^&*()/\[\]\\\|{};':"./>?,<~`+=_\-0-9A-z \u00C0-\u00ff]+)$/.test(key.toString())) {
        input += key.toString();
        inputx += 1;
      }
    }
  });

  let outputChain = Promise.resolve();

  // capture exit
  charm.removeAllListeners('^C');
  charm.on('^C', () => {
    // stop output
    outputChain = new Promise((res, rej) => {});
    // restore output coordinate for style
    charm.position(outputx, outputy);
    unpatch();
    processor({ kind: 'exit' });
  });

  // enqueue (chain) operations
  let patch = fn => (...args) => outputChain = outputChain.then(() =>
    new Promise((resolve, reject) => {
      // restore output coordinate
      charm.position(outputx, outputy);
      // write our log, then clear first line for command input
      fn(...args);
      // async get cursor position
      charm.position((x, y) => {
        outputx = x;
        outputy = y;
        // clear
        charm.position(0, 0);
        charm.erase('line');
        charm.write('SDFS> ');
        charm.write(input);
        // continue the chain
        resolve();
      });
    }));

  let unpatch = (() => {
    let oldDebug = console.debug;
    let oldLog = console.log;
    let oldError = console.error;
    let oldInfo = console.info;
    return () => {
      console.debug = oldDebug;
      console.log = oldLog;
      console.error = oldError;
      console.info = oldInfo;
    }
  })();

  console.debug = patch(console.debug);
  console.log = patch(console.log);
  console.error = patch(console.error);
  console.info = patch(console.info);

};