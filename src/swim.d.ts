declare module 'swim' {

  export = Swim;

  interface Member {
    meta: any,
    state: Swim.Member.State,
    host: string,
    incarnation: number
  }

  class Swim {
    constructor(opts: any);
    bootstrap(hostsToJoin: string[], callback: (x: string) => void);
    whoami(): string;
    members(): Member[];
    on(type: string, callback: (x: Member) => void);
  }

  namespace Swim {
    namespace Member {
        enum State {
        Alive = 0,
        Suspect = 1,
        Faulty = 2
      }
    }
    namespace EventType {
      export const Change: string;
      export const Update: string;
      export const Error: string;
      export const Ready: string;
    }
  }
  
}
