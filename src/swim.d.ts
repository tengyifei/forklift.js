declare module 'swim' {

  export = Swim;

  enum MemberState {
    Alive = 0,
    Suspect = 1,
    Faulty = 2
  }

  interface Member {
    meta: any,
    state: MemberState,
    host: string,
    incarnation: number
  }

  class Swim {
    constructor(opts: any);
    bootstrap(hostsToJoin: string[], callback: (x: string) => void);
    whoami(): string;
    members(): Member[];
    on(type: string, callback: (x: Member) => void);
    join(hosts: string[], callback?: any);
    leave();
  }

  namespace Swim {
    namespace EventType {
      export const Change: string;
      export const Update: string;
      export const Error: string;
      export const Ready: string;
    }
  }
  
}
