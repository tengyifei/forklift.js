
'use strict';

/**
 * @name eq
 * @description Compare two objects :
 *     1. if objects implement Eq, defer to their .equals
 *     2. if are arrays, iterate and recur
 * @function
 * @param {any} a Any object.
 * @param {any} b Any object.
 * @returns {boolean} In case 1, the `.equals()` function returned value.
 *     In case 2, true if each elements are equals, false otherwise.
 */
export function eq(a: any, b: any) {
    var idx = 0;
    if (a === b) {
        return true;
    }
    if (typeof a.equals === 'function') {
        return a.equals(b);
    }
    if (a.length > 0 && a.length === b.length) {
        for (; idx < a.length; idx += 1) {
            if (!eq(a[idx], b[idx])) {
                return false;
            }
        }
        return true;
    }
    return false;
}

/**
 * @name Eq
 * @description Define a contract to compare (in)equalities between
 *     objects.
 */
export interface Eq<T> {
    /**
     * @name equals
     * @description Determine if two objects are equals.
     * @methodOf Eq
     * @public
     * @param {T} The object to compare with.
     * @returns {boolean} True if the objects are equals, false otherwise.
     */
    equals(t: T): boolean;
}

export interface Monad<T> {
    /**
     * @name unit
     * @description Wrap an object inside a monad.
     * @methodOf Monad#
     * @public
     * @param {U} t The object to wrap.
     * @returns {Monad<U>} A Monad with the value wrapped inside.
     */
    unit<U>(t: U): Monad<U>;

    /**
     * @name bind
     * @description Apply the function passed as parameter on the object.
     * @methodOf Monad#
     * @public
     * @param {(t: T) => Monad<U>} f Function applied on the Monad content.
     * @returns {Monad<U>} The result of the function f wrapped inside
     *     a Monad object.
     */
    bind<U>(f: (t: T) => Monad<U>): Monad<U>;

    /**
     * @name of
     * @description Alias for unit. Fantasy Land Monad conformance.
     * @methodOf Monad#
     * @public
     * @see Monad#unit
     */
    of<U>(t: U): Monad<U>;

    /**
     * @name chain
     * @description Alias for bind. Fantasy Land Monad conformance.
     * @methodOf Monad#
     * @public
     * @see Monad#bind
     */
    chain<U>(f: (t: T) => Monad<U>): Monad<U>;
}

/**
 * @name Functor
 * @description Define a contract to add basic functor functions to an
 *     object.
 */
export interface Functor<T> {
    /**
     * @name fmap
     * @description Apply the function passed as parameter on the object.
     * @methodOf Functor#
     * @public
     * @param {(t: T) => U} f Function applied on the functor content.
     * @returns {Functor<U>} The result of the function f wrapped inside
     *     an Functor object.
     * @see Functor#fmap
     */
    fmap<U>(f: (t: T) => U): Functor<U>;

    /**
     * @name lift
     * @description Alias for fmap.
     * @methodOf Functor#
     * @public
     * @see Functor#fmap
     */
    lift<U>(f: (t: T) => U): Functor<U>;

    /**
     * @name map
     * @description Alias for fmap. Fantasy Land Monad conformance.
     * @methodOf Functor#
     * @public
     * @see Functor#fmap
     */
    map<U>(f: (t: T) => U): Functor<U>;
}

// Not yet used
interface Monoid<T> {
    mempty: Monoid<T>;
    mappend(t: Monoid<T>): Monoid<T>;
    mconcat(t: Monoid<T>[]): Monoid<T>;
}

// Not yet used
interface MonadPlus<T> extends Monad<T> {
    mzero: Monad<T>;
    mplus(t: Monad<T>): Monad<T>;
}

/**
 * @name MaybeType
 * @description Enumerate the different types contained by an Maybe object.
 * @see Maybe#
 */
export enum MaybeType { Nothing, Just }

/**
 * @name MaybePatterns
 * @description Define a contract to unwrap Maybe object using callbacks
 *     for Just and Nothing.
 * @see Maybe#
 */
export interface MaybePatterns<T,U> {
    /**
     * @name just
     * @description Function to handle the Just.
     * @type {(t: T) => U}
     */
    just: (t: T) => U;

    /**
     * @name nothing
     * @description Function to handle the Nothing.
     * @type {() => U}
     */
    nothing: () => U;
}

/**
 * @name maybe
 * @description Build a Maybe object.
 * @function
 * @param {T} t The object to wrap.
 * @returns {Maybe<T>} A Maybe object containing the input. If t is null
 *     or undefined, the Maybe object is filled with Nothing.
 * @see Maybe#
 */
export function maybe<T>(t: T) {
    return Maybe.maybe(t);
}

/**
 * @name Maybe
 * @class Encapsulates an optional value. A value of type Maybe a either
 *     contains a value of type a (represented as Just a), or it is empty
 *     (represented as Nothing).
 */
export class Maybe<T> implements Monad<T>, Functor<T>, Eq<Maybe<T>> {

    /**
     * @description Build a Maybe object. For internal use only.
     * @constructor
     * @methodOf Maybe#
     * @param {MaybeType} type Indicates if the Maybe content is a Just or a Nothing.
     * @param {T} value The value to wrap (optional).
     */
    constructor(private type: MaybeType,
                private value?: T) {}

    /**
     * @name sequence
     * @description Helper function to convert a map of Maybe objects into a Maybe of a map of objects.
     * @methodOf Maybe#
     * @static
     * @param {{[id: string]: Maybe<T>}} t The value to unwrap Maybe values from.
     * @returns {Maybe<{[id: string]: T}>} A Maybe object containing the value passed in input with fields unwrapped from Maybes.
     */
    static sequence<T>(t: {[k: string]: Maybe<T>}): Maybe<{[k: string]: T}> {
        if (Object.keys(t).filter(k => t[k].type === MaybeType.Nothing).length) {
            return Maybe.nothing<{[k: string]: T}>();
        }
        var result: {[k: string]: any} = {};
        for (var k in t) {
            if (t.hasOwnProperty(k)) {
                result[k] = t[k].value;
            }
        }
        return Maybe.just(result);
    }

    /**
     * @name all
     * @description Alias for Maybe.sequence
     * @methodOf Maybe#
     * @static
     * @see Maybe#sequence
     */
    static all = (t: {[k: string]: Maybe<any>}) => Maybe.sequence<any>(t);

    /**
     * @name maybe
     * @description Helper function to build a Maybe object.
     * @methodOf Maybe#
     * @static
     * @param {T} t The value to wrap.
     * @returns {Maybe<T>} A Maybe object containing the value passed in input. If t is null
     *     or undefined, the Maybe object is filled with Nothing.
     */
    static maybe<T>(t: T) {
        return t === null || t === undefined ?
            new Maybe<T>(MaybeType.Nothing) :
            new Maybe<T>(MaybeType.Just, t);
    }

    /**
     * @name just
     * @description Helper function to build a Maybe object filled with a
     *     Just type.
     * @methodOf Maybe#
     * @static
     * @param {T} t The value to wrap.
     * @returns {Maybe<T>} A Maybe object containing the value passed in input.
     * @throws {TypeError} If t is null or undefined.
     */
    static just<T>(t: T) {
        if (t === null || t === undefined) {
            throw new TypeError('Cannot Maybe.just(null)');
        }
        return new Maybe<T>(MaybeType.Just, t);
    }

    /**
     * @name nothing
     * @description Helper function to build a Maybe object filled with a
     *     Nothing type.
     * @methodOf Maybe#
     * @static
     * @returns {Maybe<T>} A Maybe with a Nothing type.
     */
    static nothing<T>() {
        return new Maybe<T>(MaybeType.Nothing);
    }

    /**
     * @name unit
     * @description Wrap an object inside a Maybe.
     * @public
     * @methodOf Maybe#
     * @param {U} u The object to wrap.
     * @returns {Monad<U>} A Monad with the value wrapped inside.
     * @see Monad#unit
     */
    unit<U>(u: U) {
        return Maybe.maybe<U>(u); // Slight deviation from Haskell, since sadly null does exist in JS
    }

    /**
     * @name bind
     * @description Apply the function passed as parameter on the object.
     * @methodOf Maybe#
     * @public
     * @param {(t: T) => Maybe<U>} f Function applied on the Maybe content.
     * @returns {Maybe<U>} The result of the function f wrapped inside
     *     a Maybe object.
     * @see Monad#bind
     */
    bind<U>(f: (t: T) => Maybe<U>) {
        return this.type === MaybeType.Just ?
            f(this.value) :
            Maybe.nothing<U>();
    }

    /**
     * @name of
     * @description Alias for unit.
     * @methodOf Maybe#
     * @public
     * @see Maybe#unit
     * @see Monad#of
     */
    of = this.unit;

    /**
     * @name chain
     * @description Alias for bind.
     * @methodOf Maybe#
     * @public
     * @see Maybe#unit
     * @see Monad#of
     */
    chain = this.bind;

    /**
     * @name fmap
     * @description Apply the function passed as parameter on the object.
     * @methodOf Maybe#
     * @public
     * @param {(t: T) => U} f Function applied on the Maybe content.
     * @returns {Maybe<U>} The result of the function f wrapped inside
     *     an Maybe object.
     * @see Functor#fmap
     */
    fmap<U>(f: (t: T) => U) {
        return this.bind(v => this.unit<U>(f(v)));
    }

    /**
     * @name lift
     * @description Alias for fmap.
     * @methodOf Maybe#
     * @public
     * @see Maybe#fmap
     * @see Monad#of
     */
    lift = this.fmap;

    /**
     * @name map
     * @description Alias for fmap.
     * @methodOf Maybe#
     * @public
     * @see Maybe#fmap
     * @see Monad#of
     */
    map = this.fmap;

    /**
     * @name caseOf
     * @description Execute a function depending on the Maybe content. It
     *     allows to unwrap the object for Just or Nothing types.
     * @methodOf Maybe#
     * @public
     * @param {MaybePatterns<T, U>} pattern Object containing the
     *     functions to applied on each Maybe types.
     * @return {U} The returned value of the functions specified in the
     *     MaybePatterns interface.
     * @see MaybePatterns#
     */
    caseOf<U>(patterns: MaybePatterns<T, U>) {
        return this.type === MaybeType.Just ?
            patterns.just(this.value) :
            patterns.nothing();
    }

    /**
     * @name defaulting
     * @description Convert a possible Nothing into a guaranteed Maybe.Just.
     * @methodOf Maybe#
     * @public
     * @param {T} pattern Default value to have if Nothing
     * @return {Maybe<T>}
     */
    defaulting(defaultValue: T) {
        return Maybe.just(this.valueOr(defaultValue))
    }

    /**
     * @name equals
     * @description Compare the type and the content of two Maybe
     *     objects.
     * @methodOf Maybe#
     * @public
     * @param {Maybe<T>} other The Maybe to compare with.
     * @return {boolean} True if the type and content value are equals,
     *     false otherwise.
     * @see Eq#equals
     */
    equals(other: Maybe<T>) {
        return other.type === this.type &&
            (this.type === MaybeType.Nothing || eq(other.value, this.value));
    }

    /**
     * @name valueOr
     * @description Unwrap a Maybe with a default value
     * @methodOf Maybe#
     * @public
     * @param {T} defaultValue Default value to have if Nothing
     * @return {T}
     * Separate U type to allow Maybe.nothing().valueOr() to match
     * without explicitly typing Maybe.nothing.
     */
    valueOr<U extends T>(defaultValue: U): T|U {
        return this.type === MaybeType.Just ? this.value : defaultValue;
    }
}
