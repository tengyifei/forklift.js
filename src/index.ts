import swimFuture from './swim';
import { ipToID } from './swim';
import * as Swim from 'swim';
import * as fs from 'fs';
import { Line, Exit, runConsole } from './command';
import { fileSystemProtocol } from './filesys';
import { paxos } from './paxos';
import { maplejuice } from './maplejuice';
import * as Bluebird from 'bluebird';

let terminalCommands: [RegExp, Function][] = [
  [/^help$/, () =>
    console.log(`Available commands: ${terminalCommands.map(([regex, _]) => regex.toString()).join(' ')}`)]
];

let matchSingleCommand: (x: string, y: [RegExp, Function][]) => void
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

let matchCommands: (x: string, y: [RegExp, Function][]) => Promise<void>
  = async (line, functions) => {
    let commands = line.split(';').map(x => x.trim());
    for (let i = 0; i < commands.length; i++) {
      matchSingleCommand(commands[i], functions);
      await Bluebird.delay(20);
    }
  };

runConsole(async item => {
  switch (item.kind) {
    case 'line':
      let commands = item.value.trim();
      if (commands === 'terminate') {
        process.exit(0);
      }
      await matchCommands(commands, terminalCommands);
    break;
    case 'exit':
      console.log('Shutting down');
      await Promise.race([swimFuture, Promise.resolve(null)])
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
      put(remote, () => fs.createReadStream(local))
      .then(() => console.log(`${local} uploaded as ${remote}`))],
    [/^get (\S+) (\S+)$/, (remote, local) =>
      get(remote, () => fs.createWriteStream(local))
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

paxos.then(pax => {
  console.log('Leader election initialized');
  terminalCommands = terminalCommands.concat([
    [/^leader$/, () => console.log(`Leader is: ${pax().valueOr(NaN)}`)]
  ]);
});

maplejuice.then(mj => {
  let { maple, juice } = mj;
  console.log('Map-reduce initialized');
  terminalCommands = terminalCommands.concat([
    [/^maple (\S+) (\d+) ([a-zA-Z0-9_\-]+) ([a-zA-Z0-9_\-]+)$/,
    (mapleExe, numMaples, intermediatePrefix, sourceDirectory) => {
      maple(mapleExe, +numMaples, intermediatePrefix, sourceDirectory);
    }],
    [/^juice (\S+) (\d+) ([a-zA-Z0-9_\-]+) ([a-zA-Z0-9_\-]+) delete_input=([01])( partition=(hash|range))?$/,
    (juiceExe, numJuices, intermediatePrefix, destFilename, deleteInput, _, partitionAlgorithm) => {
      juice(juiceExe, +numJuices, intermediatePrefix, destFilename, !!deleteInput, partitionAlgorithm);
    }]
  ]);
});
