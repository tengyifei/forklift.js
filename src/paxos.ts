import * as Decimal from 'decimal.js';
import * as dgram from 'dgram';
import * as msgpack from 'msgpack-lite';
import swimFuture from './swim';
import { ipToID, MemberState } from './swim';
import * as Bluebird from 'bluebird';

/**
 * Algorithm for issuing proposals
 * ===============================
 * 
 * Prepare: A proposer chooses a proposal number n and sends it to some acceptors, getting from each
 * a) A promise to never accept any proposal with number larger than n.
 * b) The proposal with highest number less than n that it has accepted.
 * 
 * Propose: Upon receiving response from a majority, the proposer proposes the highest proposal among responses,
 * or build a new proposal when none exists.
 * 
 * Algorithm for accepting proposals
 * =================================
 * 
 * Maintain the max promise number one receives, and never acknowledge any request with lower number.
 */
const PaxosPort = 41312;
type Address = string;

function makeid() {
  var text = "";
  var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  var len = 64;
  for (var i = 0; i < len; i++)
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  return text;
}

export const paxos = swimFuture.then(async swim => {
  // when UDP requests get responses, the corresponding promise is resolved
  let inFlightRequests: {[x: string]: Bluebird.Resolver<Object>} = {};

  const server = dgram.createSocket('udp4');

  interface PaxosPacket {
    id: string;
    payload: Object;
    type: 'request' | 'response';
  }

  server.on('error', async err => {
    console.error(`paxos server error:\n${err.stack}`);
    // die
    server.close();
    await Bluebird.delay(50);
    swim.leave();
    await Bluebird.delay(50);
    process.exit(-3);
  });

  server.on('message', (msg, rinfo) => {
    console.log(`paxos server got: ${msg} from ${rinfo.address}:${rinfo.port}`);
    let buffer = <Buffer> <any> msg;
    let data: PaxosPacket = msgpack.decode(buffer);
    if (data.type === 'response') {
      // our packet was acknowledged
      let request = inFlightRequests[data.id];
      if (request) {
        process.nextTick(() => request.resolve(data.payload));
        delete inFlightRequests[data.id];
      } else {
        console.warn(`paxos server: unknown UDP packet`);
      }
    } else if (data.type === 'request') {
      // we received a packet
      process.nextTick(() => onPaxosRequest(payload => {
        let id = data.id;
        let responseData: PaxosPacket = { id, payload, type: 'response' };
        server.send(msgpack.encode(responseData), PaxosPort, rinfo.address);
      }, data.payload));
    }
  });

  const sendRequest = function (address: Address, payload: Object) {
    let id = makeid();
    let data: PaxosPacket = { id, payload, type: 'request' };
    inFlightRequests[id] = Bluebird.defer();
    server.send(msgpack.encode(data), PaxosPort, address);
    return inFlightRequests[id].promise.then(x => <[Address, Object]> [address, x]);
  };

  // called when we receive a request
  // need to acknowledge by calling sendResponse
  function onPaxosRequest(sendResponse: (payload: Object) => void, payload) {

  }

  let beginListening = Bluebird.defer();
  server.on('listening', () => {
    var address = server.address();
    console.log(`paxos server listening ${address.address}:${address.port}`);
    beginListening.resolve();
  });

  server.bind(PaxosPort);

  // wait for listening event
  await beginListening.promise;


});
