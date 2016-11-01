import swimFuture from './swim';
import { Command, Exit, runConsole } from './command';

let isSwimActive = false;

let matchCommand: (x: string, y: [RegExp, Function][]) => void
  = (command, functions) => {
    for (let i = 0; i < functions.length; i++) {
      let [regex, func] = functions[i];
      let matches = regex.exec(command);
      if (matches) {
        let [_, ...captures] = matches;
        func.apply(func, captures);
        return;
      }
    }
    console.error('Unknown command: ' + command);
  };

runConsole(item => {
  switch (item.kind) {
    case 'command':
      let command = item.value;
      if (command === 'terminate') {
        process.exit(0);
      }
      matchCommand(command, [
        [/^put (.*) (.*)$/, (local, remote) => { }],
        [/^get (.*) (.*)$/, (remote, local) => { }],
        [/^delete (.*)$/, file => { }],
        [/^ls (.*)$/, file => { }],
        [/^store$/, () => { }]
      ]);
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
