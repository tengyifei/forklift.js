import swimFuture from './swim';
import * as readline from 'readline';
import * as tty from 'tty';
const charm = require('charm')(process);

(() => {

  // clears screen
  charm.reset();
  const stdin = process.stdin;
  (<tty.ReadStream> stdin).setRawMode(true);

  // cache the command
  let input = '';

  // command input
  console.log('SDFS> ');
  let outputx = 0, outputy = 1;

  let inputx = 6;

  stdin.on('data', key => {
    if (key.toString() === '\r') {
      // enter key
      input = '';
      inputx = 6;
      // clear command
      charm.position(0, 0);
      charm.erase('line');
      charm.write('SDFS> ');
    } else {
      input += key;
      inputx += 1;
    }
  });

  let patch = fn => (...args) => {
    // restore output coordinate
    charm.position(outputx, outputy);
    // write our log, then clear first line for command input
    fn(...args);
    charm.position((x, y) => {
      outputx = x;
      outputy = y;
      // clear
      charm.position(0, 0);
      charm.erase('line');
      charm.write('SDFS> ');
      charm.write(input);
    });
  };

  console.debug = patch(console.debug);
  console.log = patch(console.log);
  console.error = patch(console.error);
  console.info = patch(console.info);

})();

swimFuture.then(swim => {

  console.log('Membership protocol ready');

});
