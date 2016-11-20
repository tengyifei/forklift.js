import * as dns from 'dns';
import * as os from 'os';

let lookupTable = {};
let reverseLookupTable = {};
let myHostname = os.hostname();
let myIPAddress = '';

export function host2ip(host: string): string {
  return lookupTable[host];
}

export const stripPort = (host: string) => (/(\d+\.\d+\.\d+\.\d+):\d+/.exec(host) || [])[1] || host;

export const ip2host = (ip: string) => reverseLookupTable[stripPort(ip)];

export let myHost = () => myHostname;

export let myIP = () => myIPAddress;

export let bootstrapDNS = (() => {
  let lookup = hostname =>
    new Promise<string>((res, rej) =>
      dns.lookup(hostname, 4, (err, address) => {
        if (err) {
          console.error('Cannot resolve ' + hostname);
          rej(err);
        } else {
          setTimeout(() => res(address), 10);
        }
      }));
  return Promise.all(Array(10).fill(0).map((_, i) => i + 1) // 1..10
  .map(i => `fa16-cs425-g06-${ i < 10 ? '0' + i : i }.cs.illinois.edu`)
  .map(host =>
    lookup(host)
    .then(ip => {
      lookupTable[host] = ip;
      reverseLookupTable[ip] = host;
    })
  ))
  .then(() => {
    myIPAddress = lookupTable[myHostname];
    reverseLookupTable['127.0.0.1'] = myHostname;
  });
})();
