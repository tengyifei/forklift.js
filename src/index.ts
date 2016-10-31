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

  swim.bootstrap(hostsToJoin, err => {
    if (err) {
      // error handling
      console.error(err);
      return;
    }

    // ready
    console.log(swim.whoami());
    console.log(swim.members());

    // change on membership, e.g. new node or node died/left
    swim.on(Swim.EventType.Change, function onChange (update) {
      console.log('Change:', update);
    });

  });
});

// shutdown
// swim.leave();