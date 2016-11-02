import swimFuture from './swim';
import { ipToID } from './swim';
import * as Swim from 'swim';
import { Command, Exit, runConsole } from './command';
import { fileSystemProtocol } from './filesys';
import * as Bluebird from 'bluebird';

let terminalCommands: [RegExp, Function][] = [];

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
      matchCommand(command.trim(), terminalCommands);
    break;
    case 'exit':
      console.log('Shutting down');
      Promise.race([swimFuture, Promise.resolve(null)])
      .then(x => {
        if (x) {  // swim is active
          swimFuture
          .then(swim => swim.leave())
          .then(() => Bluebird.delay(50))
          .then(() => process.exit(0));
        } else {
          process.exit(0);
        }
      })
    break;
  }
});

swimFuture.then(swim => {
  console.log('Membership protocol ready');

  terminalCommands = terminalCommands.concat(
    [/whoami/, () => console.log(ipToID(swim.whoami()))],
    [/members/, () => console.log(`Active nodes: ${swim.members().map(ipToID).join(', ')}`)]);
});

fileSystemProtocol.then(filesys => {
  console.log('Filesystem protocol ready');
  const { put, get, del, ls, store } = filesys;

  terminalCommands = terminalCommands.concat(
    [/^put (\S+) (\S+)$/, (local, remote) => { }],
    [/^get (\S+) (\S+)$/, (remote, local) => { }],
    [/^delete (\S+)$/, file => { }],
    [/^ls (\S+)$/, file => { }],
    [/^store$/, () => { }]);
});
