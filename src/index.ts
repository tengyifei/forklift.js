import swimFuture from './swim';
import { ipToID } from './swim';
import * as Swim from 'swim';
import * as fs from 'fs';
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
      let command = item.value.trim();
      if (command === 'terminate') {
        process.exit(0);
      }
      matchCommand(command, terminalCommands);
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

  terminalCommands = terminalCommands.concat([
    [/whoami/, () => console.log(`I am: ${ipToID(swim.whoami())}`)],
    [/members/, () => console.log(`Active nodes: ${
      swim.members()
      .map(x => x.host)
      .sort()
      .map(ipToID)
      .join(', ')}`)]]);
});

fileSystemProtocol.then(filesys => {
  console.log('Filesystem protocol ready');
  const { put, get, del, ls, store } = filesys;

  terminalCommands = terminalCommands.concat([
    [/^put (\S+) (\S+)$/, (local, remote) =>
      Bluebird.promisify(fs.readFile)(local)
      .then(buff => put(remote, buff))
      .then(() => console.log(`${local} uploaded as ${remote}`))],
    [/^get (\S+) (\S+)$/, (remote, local) =>
      get(remote)
      .then(buff => (<(x: string, y: any) => Promise<void>> <any> Bluebird.promisify(fs.writeFile))(local, buff))
      .then(() => console.log(`${remote} downloaded as ${local}`))],
    [/^delete (\S+)$/, file =>
      del(file).then(() =>
        console.log(`Deleted ${file}`))],
    [/^ls (\S+)$/, file =>
      ls(file).then(nodes =>
        console.log(`Nodes containing ${file}: ${nodes.sort().join(', ')}`))],
    [/^store$/, () =>
      store().then(files =>
        console.log(`We store:\n` + files.map(x => `\t${x}`).join('\n')))]]);
});
