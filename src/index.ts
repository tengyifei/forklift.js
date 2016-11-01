import swimFuture from './swim';
import { Command, Exit, runConsole } from './command';

runConsole(item => {
  switch (item.kind) {
    case 'command':
      console.log('You typed: ' + item.value);
    break;
    case 'exit':
      console.log('Shutting down');
    break;
  }
});

swimFuture.then(swim => {
  console.log('Membership protocol ready');
  // initialize file protocol
});
