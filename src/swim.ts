/// <reference path="./swim.d.ts" />

import Swim = require('swim');
import { bootstrapDNS, myHost, myIP, host2ip, ip2host } from './resolve-name';

const port = '22894';

export enum MemberState {
  Alive = 0,
  Suspect = 1,
  Faulty = 2
}

export const ipToID = (ip: string) => (+(/fa16-cs425-g06-(\d\d).cs.illinois.edu/.exec(ip2host(ip) || '') || [])[1]) || NaN;

export default bootstrapDNS.then(() => new Promise<Swim>((resolve, reject) => {
  var opts = {
    local: {
      host: `${myIP()}:${port}`,
      meta: { 'app': 'sdfs' } // optional
    },
    codec: 'msgpack', // optional
    disseminationFactor: 15, // optional
    interval: 100, // optional
    joinTimeout: 200, // optional
    pingTimeout: 20, // optional
    pingReqTimeout: 60, // optional
    pingReqGroupSize: 3, // optional
    udp: { maxDgramSize: 768 } // optional
  };
  var swim = new Swim(opts);
  // two introducer nodes
  var hostsToJoin = myHost() === 'fa16-cs425-g06-01.cs.illinois.edu'
                  ? [`${host2ip('fa16-cs425-g06-02.cs.illinois.edu')}:${port}`]
                  : myHost() === 'fa16-cs425-g06-02.cs.illinois.edu'
                  ? [`${host2ip('fa16-cs425-g06-01.cs.illinois.edu')}:${port}`]
                  : [`${host2ip('fa16-cs425-g06-01.cs.illinois.edu')}:${port}`
                   , `${host2ip('fa16-cs425-g06-02.cs.illinois.edu')}:${port}`];

  let doBootstrap = (failed?: number) => {
    swim.bootstrap((failed || 0) > 3 ? [] : hostsToJoin, err => {
      if (err) {
        // error handling, retry
        console.error(err);
        swim.leave();
        setTimeout(() => doBootstrap((failed || 0) + 1), 950 + Math.random() * 100);
        return;
      }
      // ready
      console.log(`My IP: ${swim.whoami()}`);
      // change on membership, e.g. new node or node died/left
      swim.on(Swim.EventType.Change, function onChange (update) {
        if (update.state === MemberState.Alive) {
          console.log('Join: ' + ipToID(update.host));
        } else if (update.state === MemberState.Faulty) {
          console.log('Down: ' + ipToID(update.host));
        }
      });
      // patch members to include ourself
      swim.members = (() => {
        let oldMembersFn = <typeof swim.members> swim.members.bind(swim);
        return () => {
          let otherMembers = oldMembersFn();
          otherMembers.push({
            host: swim.whoami(),
            state: MemberState.Alive,
            meta: { 'app': 'sdfs' },
            incarnation: 0
          });
          return otherMembers;
        }
      })();
      // resolve with the bootstrapped swim object
      resolve(swim);
    });
  };
  
  doBootstrap();
}));

// shutdown
// swim.leave();