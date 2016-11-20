import * as Decimal from 'decimal.js';
import * as dgram from 'dgram';
import * as msgpack from 'msgpack-lite';
import swimFuture from './swim';
import * as Swim from 'swim';
import { ipToID, MemberState } from './swim';
import * as Bluebird from 'bluebird';
import { Maybe } from './maybe';
import { stripPort } from './resolve-name';

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

type Packet = PaxosOnewayPacket | PaxosTwowayPacket;

interface PaxosOnewayPacket {
  payload: OnewayPayload;
  type: 'oneway';
}

interface PaxosTwowayPacket {
  id: string;
  payload: PreparePayload | PrepareResponse;
  type: 'twoway';
}

interface OnewayPayload {
  index: string;
  leader: number;
  type: 'accept' | 'learn';
}

interface PreparePayload {
  index: string;
  leader: number;
  type: 'prepare';
}

interface PrepareResponse {
  highestCandidate: PreparePayload;
  type: 'prepareresponse';
}

function max <T> (input: T[], comp: (a: T, b: T) => boolean): T {
  let currMax: T;
  for (let i = 0; i < input.length; i++) {
    if (comp(input[i], currMax)) {
      currMax = input[i];
    }
  }
  return currMax;
}

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
  let inFlightRequests: {[x: string]: Bluebird.Resolver<PrepareResponse>} = {};

  const server = dgram.createSocket('udp4');

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
    let data: Packet = msgpack.decode(buffer);
    if (data.type === 'twoway') {
      let id = data.id;
      if (data.payload.type === 'prepareresponse') {
        // our packet was acknowledged
        let payload = data.payload;
        let request = inFlightRequests[data.id];
        if (request) {
          process.nextTick(() => request.resolve(payload));
          delete inFlightRequests[data.id];
        } else {
          console.warn(`paxos server: unknown UDP packet`);
        }
      } else if (data.payload.type === 'prepare') {
        // received a two-way request
        let payload = data.payload;
        process.nextTick(() => onPaxosRequest(payload => {
          let responseData: Packet = { id, payload, type: 'twoway' };
          server.send(msgpack.encode(responseData), PaxosPort, rinfo.address);
        }, payload));
      }
    } else if (data.type === 'oneway') {
      // we received a packet
      let payload = data.payload;
      process.nextTick(() => onPaxosOnewayRequest(payload));
    }
  });

  function sendTwowayRequest(address: Address, payload: PreparePayload) {
    let id = makeid();
    let data: Packet = { id, payload, type: 'twoway' };
    inFlightRequests[id] = Bluebird.defer<PrepareResponse>();
    server.send(msgpack.encode(data), PaxosPort, address);
    return inFlightRequests[id].promise
    .then(x => Maybe.just(<[Address, PrepareResponse]> [address, x]))
    .timeout(500 + Math.random() * 300)
    .catch(err => Maybe.nothing<[Address, PrepareResponse]>());
  }

  function sendOnewayRequest(address: Address, payload: OnewayPayload) {
    let data: Packet = { payload, type: 'oneway' };
    server.send(msgpack.encode(data), PaxosPort, address);
  }

  // called when we receive a request
  // need to acknowledge by calling sendResponse
  function onPaxosRequest(sendResponse: (payload: PrepareResponse) => void, payload: PreparePayload) {
    let index = new Decimal(payload.index);
    if (index.comparedTo(maxPromiseIndex) > 0) {
      maxPromiseIndex = index;
      maxPrepareRequest = payload;
    }
    // ack with highest prepare request
    sendResponse({
      highestCandidate: maxPrepareRequest,
      type: 'prepareresponse'
    });
  }

  function onPaxosOnewayRequest(payload: OnewayPayload) {
    // don't accept if we promised not to
    let index = new Decimal(payload.index);
    if (index.comparedTo(maxPromiseIndex) < 0) {
      // do nothing
    } else {
      maxPromiseIndex = index;
      if (payload.type === 'accept') {
        // accept it
        updateLeaderId(payload.leader);
        swim.members().map(x => stripPort(x.host))
        .forEach(addr => sendOnewayRequest(addr, {
          type: 'learn',
          index: payload.index,
          leader: payload.leader
        }));
      } else {
        // learn it
        updateLeaderId(payload.leader);
      }
    }
  }

  // value to be synchronized
  let currentLeaderId = Maybe.nothing<number>();
  let maxPromiseIndex = new Decimal(0);
  let maxPrepareRequest: PreparePayload;
  let ourPromiseIndex = new Decimal(Math.floor(Math.random() * 10));

  function updateLeaderId(id: number) {
    currentLeaderId = Maybe.just(id);
    console.log(`Leader is ${id}`);
  }

  async function tryProposeLeader(id: number) {
    let members = swim.members().slice();
    let prepareRequest: PreparePayload = {
      type: 'prepare',
      leader: id,
      index: ourPromiseIndex.toString()
    };
    // try proposing
    let responses =
    await Bluebird.all(members
    .map(member => stripPort(member.host))
    .map(addr => sendTwowayRequest(addr, prepareRequest)));

    let validResponses = (<[Address, PrepareResponse][]> []).concat(...responses.map(resp =>
      resp.caseOf<[Address, PrepareResponse][]>({ just: x => [x], nothing: () => [] })));
    console.log(validResponses.length);
    // check majority
    if (validResponses.length * 2 > members.length && members.length > 0) {
      // find request with highest index
      let highest = max(validResponses.map(x => x[1]), (a, b) => {
        if (a.highestCandidate) {
          if (b.highestCandidate) {
            let indexA = new Decimal(a.highestCandidate.index);
            let indexB = new Decimal(b.highestCandidate.index);
            return indexA.comparedTo(indexB) > 0;
          } else {
            return true;
          }
        } else {
          return false;
        }
      });
      let highestCandidate = highest ? highest.highestCandidate : prepareRequest;
      // send accept request with that
      validResponses.map(x => x[0])
      .forEach(addr => sendOnewayRequest(addr, {
        type: 'accept',
        index: highestCandidate.index,
        leader: highestCandidate.leader
      }));
      updateLeaderId(highestCandidate.leader);
    } else {
      // up our promise index
      ourPromiseIndex = ourPromiseIndex.plus(Math.floor(Math.random() * 10));
    }
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
  // delay a random time period
  await Bluebird.delay(Math.random() * 100 + 10);
  // begin our initial proposal
  let memberChanging = false;
  let proposeCurrent = () => {
    if (memberChanging === false) {
      tryProposeLeader(currentLeaderId.valueOr(ipToID(swim.whoami())));
    }
    setTimeout(proposeCurrent, 950 + Math.random() * 100);
  };
  proposeCurrent();

  // prepare to re-elect leader once it goes offline
  swim.on(Swim.EventType.Change, update => {
    if (update.state === MemberState.Faulty
     && currentLeaderId.fmap(x => x === ipToID(update.host)).valueOr(false)) {
      // select a new leader randomly and propagate
      let members = swim.members();
      let newLeader = members[Math.floor(Math.random() * members.length)];

      memberChanging = true;
      tryProposeLeader(ipToID(newLeader.host))
      .then(_ => memberChanging = false)
      .catch(err => memberChanging = false);
    }
  });

  return function getLeader() {
    return currentLeaderId;
  }

});
