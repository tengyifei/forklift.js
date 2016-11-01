import swimFuture from './swim';
import { Command, Exit, runConsole } from './command';

let isSwimActive = false;

runConsole(item => {
  switch (item.kind) {
    case 'command':
      console.log('You typed: ' + item.value);
    break;
    case 'exit':
      console.log('Shutting down');
      if (isSwimActive) {
        swimFuture
        .then(swim => swim.leave())
        .then(() => process.exit(0));
      } else {
        process.exit(0);
      }
    break;
  }
});

swimFuture.then(swim => {
  isSwimActive = true;
  console.log('Membership protocol ready');
  // initialize file protocol
});
