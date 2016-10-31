import * as Swim from 'swim';

var opts = {
    local: {
        host: '127.0.0.1:22894',
        meta: {'application': 'info'} // optional
    },
    codec: 'msgpack', // optional
    disseminationFactor: 15, // optional
    interval: 100, // optional
    joinTimeout: 200, // optional
    pingTimeout: 20, // optional
    pingReqTimeout: 60, // optional
    pingReqGroupSize: 3, // optional
    udp: {maxDgramSize: 512} // optional
};
var swim = new Swim(opts);
var hostsToJoin = ['127.0.0.1:22894'];

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

    // shutdown
    swim.leave();
});
