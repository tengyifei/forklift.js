/// <reference path="./swim.d.ts" />

import Swim = require('swim');
import { bootstrapDNS, myHost, myIP, host2ip, ip2host } from './resolve-name';

const port = '22894';

bootstrapDNS.then(() => {
  var opts = {
    local: {
      host: `${myIP()}:${port}`,
      meta: { 'application': 'sdfs' } // optional
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
  var hostsToJoin = myHost() === 'fa16-cs425-g06-01.cs.illinois.edu'
                  ? []
                  : [`${host2ip('fa16-cs425-g06-01.cs.illinois.edu')}:${port}`];

  let beautify = ip => (+(/fa16-cs425-g06-(\d\d).cs.illinois.edu/.exec(ip2host(ip) || '') || [])[1]) || ip;

  let doBootstrap = () => {
    swim.bootstrap(hostsToJoin, err => {
      if (err) {
        // error handling, retry
        console.error(err);
        setTimeout(doBootstrap, 1000);
        return;
      }
      // ready
      console.log(`My IP: ${swim.whoami()}`);
      // change on membership, e.g. new node or node died/left
      swim.on(Swim.EventType.Change, function onChange (update) {
        if (update.state === Swim.Member.State.Alive) {
          console.log('Join: ' + beautify(update.host));
        } else if (update.state === Swim.Member.State.Faulty) {
          console.log('Down: ' + beautify(update.host));
        } 
      });
    });
  };
});

// shutdown
// swim.leave();