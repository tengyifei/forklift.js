import * as Swim from 'swim';
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
  var hostsToJoin = [`${host2ip('fa16-cs425-g06-01.cs.illinois.edu')}:${port}`];

  swim.bootstrap(hostsToJoin, err => {
    if (err) {
      // error handling
      return;
    }

    // ready
    console.log(swim.whoami());
    console.log(swim.members());
    console.log(swim.checksum());

    // change on membership, e.g. new node or node died/left
    swim.on(Swim.EventType.Change, function onChange(update) {
      console.log('Change:', update);
    });
    // update on membership, e.g. node recovered or update on meta data
    swim.on(Swim.EventType.Update, function onUpdate(update) {
      console.log('Update:', update);
    });

  });
});

// shutdown
// swim.leave();