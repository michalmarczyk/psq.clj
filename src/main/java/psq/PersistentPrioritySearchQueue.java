package psq;

import clojure.lang.AFn;
import clojure.lang.APersistentMap;
import clojure.lang.Box;
import clojure.lang.Cons;
import clojure.lang.Indexed;
import clojure.lang.IObj;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.LazySeq;
import clojure.lang.MapEntry;
import clojure.lang.PersistentList;
import clojure.lang.PersistentVector;
import clojure.lang.Reversible;
import clojure.lang.RT;
import clojure.lang.Sorted;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 * Persistent Priority Search Queues implemented using Ralf Hinze's priority
 * search pennants, see R. Hinze, A Simple Implementation Technique for Priority
 * Search Queues.
 */

public final class PersistentPrioritySearchQueue
        extends APersistentMap
        implements Indexed, IObj, IPrioritySearchQueue, Reversible, Sorted {

    static public final PersistentPrioritySearchQueue EMPTY =
            new PersistentPrioritySearchQueue();

    public final Winner winner;

    public final Comparator kcomp;
    public final Comparator pcomp;

    final int _count;
    final IPersistentMap _meta;

    static public IPersistentMap create(Map other) {
        IPersistentMap ret = EMPTY;
        for (Object o : other.entrySet()) {
            Map.Entry e = (Entry) o;
            ret = ret.assoc(e.getKey(), e.getValue());
        }
        return ret;
    }

    static public PersistentPrioritySearchQueue create(Object... init) {
        IPersistentMap ret = EMPTY;
        for (int i = 0; i < init.length; i += 2) {
            ret = ret.assoc(init[i], init[i+1]);
        }
        return (PersistentPrioritySearchQueue) ret;
    }

    static public PersistentPrioritySearchQueue create(IPersistentMap meta,
                                                       Object... init) {
        IPersistentMap ret = (IPersistentMap) EMPTY.withMeta(meta);
        for (int i = 0; i < init.length; i += 2) {
            ret = ret.assoc(init[i], init[i+1]);
        }
        return (PersistentPrioritySearchQueue) ret;
    }

    static public PersistentPrioritySearchQueue create(Comparator kcomp,
                                                       Comparator pcomp,
                                                       Object... init) {
        IPersistentMap ret = new PersistentPrioritySearchQueue(kcomp, pcomp);
        for (int i = 0; i < init.length; i += 2) {
            ret = ret.assoc(init[i], init[i+1]);
        }
        return (PersistentPrioritySearchQueue) ret;
    }

    static public PersistentPrioritySearchQueue create(Comparator kcomp,
                                                       Comparator pcomp,
                                                       IPersistentMap meta,
                                                       Object... init) {
        IPersistentMap ret = new PersistentPrioritySearchQueue(kcomp, pcomp, meta);
        for (int i = 0; i < init.length; i += 2) {
            ret = ret.assoc(init[i], init[i+1]);
        }
        return (PersistentPrioritySearchQueue) ret;
    }

    static public PersistentPrioritySearchQueue create(ISeq items) {
        IPersistentMap ret = EMPTY;
        for (; items != null; items = items.next().next()) {
            if (items.next() == null)
                throw new IllegalArgumentException(
                        String.format("No value supplied for key: %s", items.first())
                );
            ret = ret.assoc(items.first(), RT.second(items));
        }
        return (PersistentPrioritySearchQueue) ret;
    }

    static public PersistentPrioritySearchQueue create(Comparator kcomp,
                                                       Comparator pcomp,
                                                       ISeq items) {
        IPersistentMap ret = new PersistentPrioritySearchQueue(kcomp, pcomp);
        for (; items != null; items = items.next().next()) {
            if (items.next() == null)
                throw new IllegalArgumentException(
                        String.format("No value supplied for key: %s", items.first())
                );
            ret = ret.assoc(items.first(), RT.second(items));
        }
        return (PersistentPrioritySearchQueue) ret;
    }

    PersistentPrioritySearchQueue() {
        this(RT.DEFAULT_COMPARATOR, RT.DEFAULT_COMPARATOR, null);
    }

    PersistentPrioritySearchQueue(Comparator kcomp, Comparator pcomp) {
        this(kcomp, pcomp, null);
    }

    PersistentPrioritySearchQueue(Comparator kcomp,
                                  Comparator pcomp,
                                  IPersistentMap meta) {
        this.winner = null;
        this.kcomp = kcomp;
        this.pcomp = pcomp;
        this._meta = meta;
        this._count = 0;
    }

    PersistentPrioritySearchQueue(Winner winner,
                                  Comparator kcomp,
                                  Comparator pcomp,
                                  int count,
                                  IPersistentMap meta) {
        this.winner = winner;
        this.kcomp = kcomp;
        this.pcomp = pcomp;
        this._meta = meta;
        this._count = count;
    }

    public static final class Winner {

        public final Object key;
        public final Object priority;
        public final Loser losers;
        public final Object ubound;

        Winner(Object key, Object priority, Loser losers, Object ubound) {
            this.key = key;
            this.priority = priority;
            this.losers = losers;
            this.ubound = ubound;
        }

    }

    public static final class Loser {

        public final Object key;
        public final Object priority;
        public final Loser left;
        public final Object split;
        public final Loser right;
        public final int size;

        Loser(Object key, Object priority, Loser left, Object split, Loser right, int size) {
            this.key = key;
            this.priority = priority;
            this.left = left;
            this.split = split;
            this.right = right;
            this.size = size;
        }
    }

    public static final class Match {

        public static final Match EMPTY = new Match(null, null);

        public final Winner left;
        public final Winner right;

        Match(Winner winner) {
            this(winner, null);
        }

        Match(Winner left, Winner right) {
            this.left = left;
            this.right = right;
        }
    }

    public static final class MatchFrame {

        public boolean hasLeft;
        public boolean hasRight;
        public Object lkey;
        public Object lpriority;
        public Loser llosers;
        public Object lubound;
        public Object rkey;
        public Object rpriority;
        public Loser rlosers;
        public Object rubound;
        public boolean found;

        MatchFrame() {}

        MatchFrame(Winner winner) {
            hasLeft = true;
            lkey = winner.key;
            lpriority = winner.priority;
            llosers = winner.losers;
            lubound = winner.ubound;
        }

        void setLeft(Object key, Object priority, Loser losers, Object ubound) {
            hasLeft = true;
            lkey = key;
            lpriority = priority;
            llosers = losers;
            lubound = ubound;
        }

        void clearLeft() {
            hasLeft = false;
        }

        void setRight(Object key, Object priority, Loser losers, Object ubound) {
            hasRight = true;
            rkey = key;
            rpriority = priority;
            rlosers = losers;
            rubound = ubound;
        }

        void clearRight() {
            hasRight = false;
        }

        void clear() {
            clearLeft();
            clearRight();
        }

        void shift() {
            hasLeft = hasRight;
            lkey = rkey;
            lpriority = rpriority;
            llosers = rlosers;
            lubound = rubound;
            clearRight();
        }

        void unshift() {
            hasRight = hasLeft;
            rkey = lkey;
            rpriority = lpriority;
            rlosers = llosers;
            rubound = lubound;
            clearLeft();
        }

        Winner getLeft() {
            if (hasLeft)
                return new Winner(lkey, lpriority, llosers, lubound);
            return null;
        }
    }

    // tree balancing

    static int size(Loser loser) {
        if (null == loser)
            return 0;
        return loser.size;
    }

    static Loser loser(Object key, Object priority, Loser left, Object split, Loser right) {
        return new Loser(key, priority, left, split, right, 1 + size(left) + size(right));
    }

    static int omega(int size) {
        return 4 * size;
    }

    Loser singleLeft(Object key, Object priority, Loser left, Object split, Loser right) {
        Object rkey = right.key;
        Object rpriority = right.priority;
        Loser rleft = right.left;
        Object rsplit = right.split;
        Loser rright = right.right;
        if (kcomp.compare(rkey, rsplit) <= 0
                && pcomp.compare(priority, rpriority) <= 0)
            return loser(
                    key,
                    priority,
                    loser(rkey, rpriority, left, split, rleft),
                    rsplit,
                    rright
            );
        return loser(
                rkey,
                rpriority,
                loser(key, priority, left, split, rleft),
                rsplit,
                rright
        );
    }

    Loser singleRight(Object key, Object priority, Loser left, Object split, Loser right) {
        Object lkey = left.key;
        Object lpriority = left.priority;
        Loser lleft = left.left;
        Object lsplit = left.split;
        Loser lright = left.right;
        if (kcomp.compare(lkey, lsplit) > 0
                && pcomp.compare(priority, lpriority) <= 0)
            return loser(
                    key,
                    priority,
                    lleft,
                    lsplit,
                    loser(lkey, lpriority, lright, split, right)
            );
        return loser(
                lkey,
                lpriority,
                lleft,
                lsplit,
                loser(key, priority, lright, split, right)
        );
    }

    Loser doubleLeft(Object key, Object priority, Loser left, Object split, Loser right) {
        return singleLeft(
                key, priority, left, split,
                singleRight(right.key, right.priority, right.left, right.split, right.right)
        );
    }

    Loser doubleRight(Object key, Object priority, Loser left, Object split, Loser right) {
        return singleRight(
                key, priority,
                singleLeft(left.key, left.priority, left.left, left.split, left.right),
                split, right
        );
    }

    Loser balanceLeft(Object key, Object priority, Loser left, Object split, Loser right) {
        Loser rl = right.left;
        Loser rr = right.right;
        if (size(rl) < size(rr))
            return singleLeft(key, priority, left, split, right);
        return doubleLeft(key, priority, left, split, right);
    }

    Loser balanceRight(Object key, Object priority, Loser left, Object split, Loser right) {
        Loser ll = left.left;
        Loser lr = left.right;
        if (size(lr) < size(ll))
            return singleRight(key, priority, left, split, right);
        return doubleRight(key, priority, left, split, right);
    }

    Loser balance(Object key, Object priority, Loser left, Object split, Loser right) {
        int sl = size(left);
        int sr = size(right);

        if (sl + sr < 2)
            return loser(key, priority, left, split, right);
        if (sr > omega(sl))
            return balanceLeft(key, priority, left, split, right);
        if (sl > omega(sr))
            return balanceRight(key, priority, left, split, right);
        return loser(key, priority, left, split, right);
    }

    // main ops

    Winner play(Winner left, Winner right) {
        if (null == left)
            return right;
        if (null == right)
            return left;

        Object lp = left.priority;
        Object rp = right.priority;

        if (pcomp.compare(lp, rp) <= 0)
            return new Winner(
                    left.key,
                    lp,
                    balance(
                            right.key,
                            rp,
                            left.losers,
                            left.ubound,
                            right.losers
                    ),
                    right.ubound
            );
        return new Winner(
                right.key,
                rp,
                balance(
                        left.key,
                        lp,
                        left.losers,
                        left.ubound,
                        right.losers
                ),
                right.ubound
        );
    }

    void play(MatchFrame mf) {
        if (!mf.hasLeft) {
            if (!mf.hasRight)
                return;
            mf.shift();
        }
        if (!mf.hasRight)
            return;

        Object lkey = mf.lkey;
        Object lpriority = mf.lpriority;
        Loser llosers = mf.llosers;
        Object lubound = mf.lubound;
        Object rkey = mf.rkey;
        Object rpriority = mf.rpriority;
        Loser rlosers = mf.rlosers;
        Object rubound = mf.rubound;

        if (pcomp.compare(lpriority, rpriority) <= 0) {
            mf.setLeft(
                    lkey,
                    lpriority,
                    balance(
                            rkey,
                            rpriority,
                            llosers,
                            lubound,
                            rlosers
                    ),
                    rubound
            );
        } else {
            mf.setLeft(
                    rkey,
                    rpriority,
                    balance(
                            lkey,
                            lpriority,
                            llosers,
                            lubound,
                            rlosers
                    ),
                    rubound
            );
        }
        mf.clearRight();
    }

    Match unplay(Winner winner) {
        if (null == winner)
            return Match.EMPTY;

        Loser losers = winner.losers;
        if (null == losers)
            return new Match(winner);

        Object wkey = winner.key;
        Object wpriority = winner.priority;
        Object wubound = winner.ubound;
        Object lkey = losers.key;
        Object lpriority = losers.priority;
        Loser lleft = losers.left;
        Object lsplit = losers.split;
        Loser lright = losers.right;
        if (kcomp.compare(lkey, lsplit) <= 0)
            return new Match(
                    new Winner(lkey, lpriority, lleft, lsplit),
                    new Winner(wkey, wpriority, lright, wubound)
            );
        return new Match(
                new Winner(wkey, wpriority, lleft, lsplit),
                new Winner(lkey, lpriority, lright, wubound)
        );
    }

    void unplay(MatchFrame mf) {
        if (!mf.hasLeft)
            return;

        Loser losers = mf.llosers;
        if (null == losers)
            return;

        Object wkey = mf.lkey;
        Object wpriority = mf.lpriority;
        Object wubound = mf.lubound;
        Object lkey = losers.key;
        Object lpriority = losers.priority;
        Loser lleft = losers.left;
        Object lsplit = losers.split;
        Loser lright = losers.right;
        mf.hasLeft = true;
        mf.hasRight = true;
        mf.llosers = lleft;
        mf.lubound = lsplit;
        mf.rlosers = lright;
        mf.rubound = wubound;
        if (kcomp.compare(lkey, lsplit) <= 0) {
            mf.lkey = lkey;
            mf.lpriority = lpriority;
            mf.rkey = wkey;
            mf.rpriority = wpriority;
            return;
        }
        mf.lkey = wkey;
        mf.lpriority = wpriority;
        mf.rkey = lkey;
        mf.rpriority = lpriority;
    }

    Winner secondBest(Loser losers, Object ubound) {
        MatchFrame mf = new MatchFrame();
        secondBest(losers, ubound, mf);
        return mf.getLeft();
    }

    void secondBest(Loser losers, Object ubound, MatchFrame mf) {
        if (null == losers)
            return;

        Object key = losers.key;
        Object priority = losers.priority;
        Loser left = losers.left;
        Object split = losers.split;
        Loser right = losers.right;
        if (kcomp.compare(key, split) <= 0) {
            secondBest(right, ubound, mf);
            mf.unshift();
            mf.setLeft(key, priority, left, split);
        } else {
            secondBest(left, split, mf);
            mf.setRight(key, priority, right, ubound);
        }
        play(mf);
    }

    public ISeq prioritySeq() {
        if (0 == _count)
            return null;

        return new Cons(
                new MapEntry(winner.key, winner.priority),
                prioritySeq(_count - 1, winner.losers, winner.ubound)
        );
    }

    ISeq prioritySeq(final int countdown, final Loser losers, final Object ubound) {
        if (0 == countdown)
            return null;

        return new LazySeq(
                new AFn() {
                    public Object invoke() {
                        MatchFrame mf = new MatchFrame();
                        secondBest(losers, ubound, mf);
                        Object lkey = mf.lkey;
                        Object lpriority = mf.lpriority;
                        Loser llosers = mf.llosers;
                        Object lubound = mf.lubound;
                        return new Cons(
                                new MapEntry(lkey, lpriority),
                                prioritySeq(countdown - 1, llosers, lubound)
                        );
                    }
                }
        );
    }

    Winner delete(Object key, Winner winner, Box found) {
        if (null == winner)
            return null;
        if (null == winner.losers) {
            if (0 == kcomp.compare(key, winner.key)) {
                found.val = found;
                return null;
            } else {
                return winner;
            }
        }
        Match match = unplay(winner);
        Object lubound = match.left.ubound;
        int c = kcomp.compare(key, lubound);
        Winner sub = delete(key, c <= 0 ? match.left : match.right, found);
        if (null == found.val)
            return winner;
        if (c <= 0)
            return play(sub, match.right);
        return play(match.left, sub);
    }

    Winner insert(Object key, Object priority, Winner winner, Box found) {
        if (null == winner)
            return new Winner(key, priority, null, key);

        if (null == winner.losers) {
            Winner newWinner = new Winner(key, priority, null, key);
            Object wkey = winner.key;
            int c = kcomp.compare(key, wkey);
            if (c < 0)
                return play(newWinner, winner);
            if (c == 0) {
                found.val = found;
                return newWinner;
            }
            return play(winner, newWinner);
        }

        Match match = unplay(winner);
        Object lubound = match.left.ubound;
        if (kcomp.compare(key, lubound) <= 0)
            return play(
                    insert(key, priority, match.left, found),
                    match.right
            );
        return play(
                match.left,
                insert(key, priority, match.right, found)
        );
    }

    Winner insert(Object key, Object priority, Winner winner, MatchFrame mf) {
        if (null == winner)
            return new Winner(key, priority, null, key);

        mf.setLeft(winner.key, winner.priority, winner.losers, winner.ubound);
        insert(key, priority, mf);
        return mf.getLeft();
    }

    void insert(Object key, Object priority, MatchFrame mf) {
        if (null == mf.llosers) {
            Object wkey = mf.lkey;
            int c = kcomp.compare(key, wkey);
            if (0 == c) {
                mf.found = true;
                mf.setLeft(key, priority, null, key);
            } else if (c < 0) {
                mf.unshift();
                mf.setLeft(key, priority, null, key);
                play(mf);
            } else {
                mf.setRight(key, priority, null, key);
                play(mf);
            }
            return;
        }

        unplay(mf);
        Object lkey = mf.lkey;
        Object lpriority = mf.lpriority;
        Loser llosers = mf.llosers;
        Object lubound = mf.lubound;
        Object rkey = mf.rkey;
        Object rpriority = mf.rpriority;
        Loser rlosers = mf.rlosers;
        Object rubound = mf.rubound;
        if (kcomp.compare(key, lubound) <= 0) {
            mf.clearRight();
            insert(key, priority, mf);
            mf.setRight(rkey, rpriority, rlosers, rubound);
        } else {
            mf.shift();
            insert(key, priority, mf);
            mf.unshift();
            mf.setLeft(lkey, lpriority, llosers, lubound);
        }
        play(mf);
    }

    MapEntry lookup(Object key, Winner winner) {
        if (null == winner)
            return null;

        if (0 == kcomp.compare(key, winner.key))
            return new MapEntry(winner.key, winner.priority);

        Loser losers = winner.losers;
        while (null != losers) {
            if (0 == kcomp.compare(key, losers.key))
                return new MapEntry(losers.key, losers.priority);
            if (0 < kcomp.compare(key, losers.split)) {
                losers = losers.right;
                continue;
            }
            losers = losers.left;
        }
        return null;
    }

    public MapEntry nearestLeft(Object key, boolean inclusive) {
        if (null == winner)
            return null;

        int topc = kcomp.compare(key, winner.ubound);
        if (0 == topc) {
            if (inclusive)
                return lookup(key, winner);
            Loser loser = winner.losers;
            if (null == loser) {
                return null;
            }
            while (null != loser.right) {
                loser = loser.right;
            }
            return lookup(loser.split, winner);
        } else if (topc > 0) {
            return lookup(winner.ubound, winner);
        }

        Loser prev = null;
        Loser loser = winner.losers;
        while (null != loser) {
            int c = kcomp.compare(key, loser.split);
            if (0 == c) {
                if (inclusive)
                    return lookup(key, winner);
                loser = loser.left;
            } else if (c < 0) {
                loser = loser.left;
            } else {
                prev = loser;
                loser = loser.right;
            }
        }
        if (null == prev)
            return null;
        return lookup(prev.split, winner);
    }

    public MapEntry nearestRight(Object key, boolean inclusive) {
        if (null == winner)
            return null;

        int topc = kcomp.compare(key, winner.ubound);
        if (0 == topc) {
            if (inclusive)
                return lookup(key, winner);
            return null;
        } else if (topc > 0) {
            return null;
        }

        Object prevSplit = winner.ubound;
        Loser loser = winner.losers;
        while (null != loser) {
            int c = kcomp.compare(key, loser.split);
            if (0 == c) {
                if (inclusive)
                    return lookup(key, winner);
                loser = loser.right;
            } else if (c > 0) {
                loser = loser.right;
            } else {
                prevSplit = loser.split;
                loser = loser.left;
            }
        }
        return lookup(prevSplit, winner);
    }

    ISeq traverse(final Object key, final Object priority, Loser losers) {
        if (null == losers)
            return RT.list(new MapEntry(key, priority));
        final Object lkey = losers.key;
        final Object lpriority = losers.priority;
        final Loser lleft = losers.left;
        final Loser lright = losers.right;
        if (kcomp.compare(lkey, losers.split) <= 0)
            return concat(
                    new LazySeq(new AFn() {
                        public ISeq invoke() {
                            return traverse(lkey, lpriority, lleft);
                        }
                    }),
                    new LazySeq(new AFn() {
                        public ISeq invoke() {
                            return traverse(key, priority, lright);
                        }
                    })
            );
        return concat(
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return traverse(key, priority, lleft);
                    }
                }),
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return traverse(lkey, lpriority, lright);
                    }
                })
        );
    }

    ISeq rtraverse(final Object key, final Object priority, Loser losers) {
        if (null == losers)
            return RT.list(new MapEntry(key, priority));
        final Object lkey = losers.key;
        final Object lpriority = losers.priority;
        final Loser lleft = losers.left;
        final Loser lright = losers.right;
        if (kcomp.compare(lkey, losers.split) <= 0)
            return concat(
                    new LazySeq(new AFn() {
                        public ISeq invoke() {
                            return rtraverse(key, priority, lright);
                        }
                    }),
                    new LazySeq(new AFn() {
                        public ISeq invoke() {
                            return rtraverse(lkey, lpriority, lleft);
                        }
                    })
            );
        return concat(
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return rtraverse(lkey, lpriority, lright);
                    }
                }),
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return rtraverse(key, priority, lleft);
                    }
                })
        );
    }

    ISeq traverseFrom(final Object lbound, Winner winner) {
        if (null == winner)
            return PersistentList.EMPTY;
        final Match match = unplay(winner);
        if (null == match.right) {
            Object lkey = match.left.key;
            if (kcomp.compare(lbound, lkey) <= 0)
                return RT.list(new MapEntry(lkey, match.left.priority));
            return PersistentList.EMPTY;
        }
        final Object lubound = match.left.ubound;
        return concat(
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return kcomp.compare(lbound, lubound) <= 0 ?
                                traverseFrom(lbound, match.left) :
                                null;
                    }
                }),
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return traverseFrom(lbound, match.right);
                    }
                })
        );
    }

    ISeq rtraverseFrom(final Object ubound, Winner winner) {
        if (null == winner)
            return PersistentList.EMPTY;
        final Match match = unplay(winner);
        if (null == match.right) {
            Object lkey = match.left.key;
            if (kcomp.compare(lkey, ubound) <= 0)
                return RT.list(new MapEntry(lkey, match.left.priority));
            return PersistentList.EMPTY;
        }
        final Object lubound = match.left.ubound;
        return concat(
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return kcomp.compare(lubound, ubound) <= 0 ?
                                rtraverseFrom(ubound, match.right) :
                                null;
                    }
                }),
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return rtraverseFrom(ubound, match.left);
                    }
                })
        );
    }

    // bounded-priority traversals

    ISeq traverseAtMost(final Object priority, final Winner winner) {
        if (null == winner || pcomp.compare(winner.priority, priority) > 0)
            return PersistentList.EMPTY;
        final Match match = unplay(winner);
        if (null == match.right)
            return RT.list(new MapEntry(match.left.key, match.left.priority));
        return concat(
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return traverseAtMost(priority, match.left);
                    }
                }),
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return traverseAtMost(priority, match.right);
                    }
                })
        );
    }

    ISeq traverseBelow(final Object priority, final Winner winner) {
        if (null == winner || pcomp.compare(winner.priority, priority) >= 0)
            return PersistentList.EMPTY;
        final Match match = unplay(winner);
        if (null == match.right)
            return RT.list(new MapEntry(match.left.key, match.left.priority));
        return concat(
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return traverseBelow(priority, match.left);
                    }
                }),
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return traverseBelow(priority, match.right);
                    }
                })
        );
    }

    ISeq rtraverseAtMost(final Object priority, Winner winner) {
        if (null == winner || pcomp.compare(winner.priority, priority) > 0)
            return PersistentList.EMPTY;
        final Match match = unplay(winner);
        if (null == match.right)
            return RT.list(new MapEntry(match.left.key, match.left.priority));
        return concat(
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return rtraverseAtMost(priority, match.right);
                    }
                }),
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return rtraverseAtMost(priority, match.left);
                    }
                })
        );
    }

    ISeq rtraverseBelow(final Object priority, Winner winner) {
        if (null == winner || pcomp.compare(winner.priority, priority) >= 0)
            return PersistentList.EMPTY;
        final Match match = unplay(winner);
        if (null == match.right)
            return RT.list(new MapEntry(match.left.key, match.left.priority));
        return concat(
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return rtraverseBelow(priority, match.right);
                    }
                }),
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return rtraverseBelow(priority, match.left);
                    }
                })
        );
    }

    ISeq traverseAtMostRange(final Object low, final Object high, final Object priority, Winner winner) {
        if (null == winner || pcomp.compare(winner.priority, priority) > 0)
            return PersistentList.EMPTY;
        final Match match = unplay(winner);
        if (null == match.right) {
            Object lkey = match.left.key;
            if (kcomp.compare(low, lkey) <= 0 && kcomp.compare(lkey, high) <= 0)
                return RT.list(new MapEntry(lkey, match.left.priority));
            return PersistentList.EMPTY;
        }
        final Object lubound = match.left.ubound;
        return concat(
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return kcomp.compare(low, lubound) <= 0 ?
                                traverseAtMostRange(low, high, priority, match.left) :
                                null;
                    }
                }),
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return kcomp.compare(lubound, high) <= 0 ?
                                traverseAtMostRange(low, high, priority, match.right) :
                                null;
                    }
                })
        );
    }

    ISeq traverseBelowRange(final Object low, final Object high, final Object priority, Winner winner) {
        if (null == winner || pcomp.compare(winner.priority, priority) >= 0)
            return PersistentList.EMPTY;
        final Match match = unplay(winner);
        if (null == match.right) {
            Object lkey = match.left.key;
            if (kcomp.compare(low, lkey) <= 0 && kcomp.compare(lkey, high) <= 0)
                return RT.list(new MapEntry(lkey, match.left.priority));
            return PersistentList.EMPTY;
        }
        final Object lubound = match.left.ubound;
        return concat(
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return kcomp.compare(low, lubound) <= 0 ?
                                traverseBelowRange(low, high, priority, match.left) :
                                null;
                    }
                }),
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return kcomp.compare(lubound, high) <= 0 ?
                                traverseBelowRange(low, high, priority, match.right) :
                                null;
                    }
                })
        );
    }

    ISeq rtraverseAtMostRange(final Object low, final Object high, final Object priority, Winner winner) {
        if (null == winner || pcomp.compare(winner.priority, priority) > 0)
            return PersistentList.EMPTY;
        final Match match = unplay(winner);
        if (null == match.right) {
            Object lkey = match.left.key;
            if (kcomp.compare(low, lkey) <= 0 && kcomp.compare(lkey, high) <= 0)
                return RT.list(new MapEntry(lkey, match.left.priority));
            return PersistentList.EMPTY;
        }
        final Object lubound = match.left.ubound;
        return concat(
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return kcomp.compare(lubound, high) <= 0 ?
                                rtraverseAtMostRange(low, high, priority, match.right) :
                                null;
                    }
                }),
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return kcomp.compare(low, lubound) <= 0 ?
                                rtraverseAtMostRange(low, high, priority, match.left) :
                                null;
                    }
                })
        );
    }

    ISeq rtraverseBelowRange(final Object low, final Object high, final Object priority, Winner winner) {
        if (null == winner || pcomp.compare(winner.priority, priority) >= 0)
            return PersistentList.EMPTY;
        final Match match = unplay(winner);
        if (null == match.right) {
            Object lkey = match.left.key;
            if (kcomp.compare(low, lkey) <= 0 && kcomp.compare(lkey, high) <= 0)
                return RT.list(new MapEntry(lkey, match.left.priority));
            return PersistentList.EMPTY;
        }
        final Object lubound = match.left.ubound;
        return concat(
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return kcomp.compare(lubound, high) <= 0 ?
                                rtraverseBelowRange(low, high, priority, match.right) :
                                null;
                    }
                }),
                new LazySeq(new AFn() {
                    public ISeq invoke() {
                        return kcomp.compare(low, lubound) <= 0 ?
                                rtraverseBelowRange(low, high, priority, match.left) :
                                null;
                    }
                })
        );
    }
    // IPrioritySearchQueue

    public ISeq atMost(Object priority) {
        return traverseAtMost(priority, winner);
    }

    public ISeq below(Object priority) {
        return traverseBelow(priority, winner);
    }

    public ISeq atMostRange(Object low, Object high, Object priority) {
        return traverseAtMostRange(low, high, priority, winner);
    }

    public ISeq belowRange(Object low, Object high, Object priority) {
        return traverseBelowRange(low, high, priority, winner);
    }

    public ISeq reverseAtMost(Object priority) {
        return rtraverseAtMost(priority, winner);
    }

    public ISeq reverseBelow(Object priority) {
        return rtraverseBelow(priority, winner);
    }

    public ISeq reverseAtMostRange(Object low, Object high, Object priority) {
        return rtraverseAtMostRange(low, high, priority, winner);
    }

    public ISeq reverseBelowRange(Object low, Object high, Object priority) {
        return rtraverseBelowRange(low, high, priority, winner);
    }

    // split

    public PersistentVector split(Object splitKey) {
        if (isEmpty()) {
            Object empty = empty().withMeta(null);
            return PersistentVector.create(empty, null, empty);
        }

        Box splitEntry = new Box(null);
        Match split = split(winner, splitKey, splitEntry);
        Object left = null, right = null;
        int lsize = split.left == null ? 0 : rank(split.left.ubound) + 1;
        int rsize = _count - lsize;
        if (splitEntry.val != null)
            rsize -= 1;
        if (split.left != null)
            left = new PersistentPrioritySearchQueue(split.left, kcomp, pcomp, lsize, null);
        else
            left = empty().withMeta(null);
        if (split.right != null)
            right = new PersistentPrioritySearchQueue(split.right, kcomp, pcomp, rsize, null);
        else
            right = empty().withMeta(null);
        return PersistentVector.create(left, splitEntry.val, right);
    }

    Match split(Winner winner, Object splitKey, Box splitEntry) {
        if (winner == null)
            return Match.EMPTY;
        if (kcomp.compare(splitKey, winner.key) == 0) {
            splitEntry.val = lookup(splitKey, winner);
            winner = delete(splitKey, winner, new Box(null));
            if (winner == null)
                return Match.EMPTY;
        }
        if (winner.losers == null) {
            if (kcomp.compare(splitKey, winner.key) <= 0)
                return new Match(null, winner);
            return new Match(winner);
        }

        Match match = unplay(winner);
        if (kcomp.compare(splitKey, match.left.ubound) <= 0) {
            Match sub = split(match.left, splitKey, splitEntry);
            return new Match(sub.left, play(sub.right, match.right));
        } else {
            Match sub = split(match.right, splitKey, splitEntry);
            return new Match(play(match.left, sub.left), sub.right);
        }
    }

    // rank

    public int rank(Object key) {
        if (isEmpty())
            return -1;
        if (kcomp.compare(winner.ubound, key) == 0)
            return _count - 1;
        int rank = 0;
        for (Loser loser = winner.losers; loser != null; ) {
            int c = kcomp.compare(key, loser.split);
            if (c < 0) {
                loser = loser.left;
                continue;
            } else if (c > 0) {
                rank += size(loser.left) + 1;
                loser = loser.right;
                continue;
            }
            return rank + size(loser.left);
        }
        return -1;
    }

    // helpers

    Object throwUnsupported() {
        throw new UnsupportedOperationException();
    }

    static ISeq concat(ISeq xs, ISeq ys) {
        // NB. relying on an implementation detail
        return (ISeq) clojure.core$concat.invokeStatic(xs, ys);
    }

    // clojure.lang.Associative

    public PersistentPrioritySearchQueue assoc(Object k, Object p) {
//        Box found = new Box(null);
//        Winner newWinner = insert(k, p, winner, found);
        MatchFrame mf = new MatchFrame();
        Winner newWinner = insert(k, p, winner, mf);
        return new PersistentPrioritySearchQueue(
                newWinner,
                kcomp,
                pcomp,
                // found.val != null ? _count : _count + 1,
                mf.found ? _count : _count + 1,
                _meta
        );
    }

    public boolean containsKey(Object k) {
        return null != entryAt(k);
    }

    public MapEntry entryAt(Object k) {
        return lookup(k, winner);
    }

    // clojure.lang.Counted

    public int count() {
        return _count;
    }

    // clojure.lang.ILookup

    public Object valAt(Object k) {
        return valAt(k, null);
    }

    public Object valAt(Object k, Object notFound) {
        if (null == winner)
            return notFound;

        if (0 == kcomp.compare(k, winner.key))
            return winner.priority;

        Loser losers = winner.losers;
        while (null != losers) {
            if (0 == kcomp.compare(k, losers.key))
                return losers.priority;
            if (0 < kcomp.compare(k, losers.split)) {
                losers = losers.right;
                continue;
            }
            losers = losers.left;
        }
        return notFound;
    }

    // clojure.lang.IMeta

    public IPersistentMap meta() {
        return _meta;
    }

    // clojure.lang.Indexed

    public Object nth(int i) {
        return nth(i, null);
    }

    public Object nth(int i, Object notFound) {
        if (i < 0 || i >= _count)
            throw new IndexOutOfBoundsException();
        if (i == _count - 1)
            return entryAt(winner.ubound);
        Loser loser = winner.losers;
        while (loser != null) {
            int rank = size(loser.left);
            if (i < rank) {
                loser = loser.left;
                continue;
            } else if (i > rank) {
                i -= rank + 1;
                loser = loser.right;
                continue;
            }
            return entryAt(loser.split);
        }
        return new MapEntry(winner.key, winner.priority);
    }

    // clojure.lang.IObj

    public PersistentPrioritySearchQueue withMeta(IPersistentMap meta) {
        return new PersistentPrioritySearchQueue(kcomp, pcomp, meta);
    }

    // clojure.lang.IPersistentCollection

    public PersistentPrioritySearchQueue empty() {
        return new PersistentPrioritySearchQueue(kcomp, pcomp, _meta);
    }

    // clojure.lang.IPersistentMap

    public PersistentPrioritySearchQueue without(Object k) {
        Box found = new Box(null);
        return new PersistentPrioritySearchQueue(
                delete(k, winner, found),
                kcomp,
                pcomp,
                found.val != null ? _count - 1 : _count,
                _meta
        );
    }

    public PersistentPrioritySearchQueue assocEx(Object k, Object p) {
        Box found = new Box(null);
        Winner newWinner = insert(k, p, winner, found);
        if (found.val != null)
            throw new RuntimeException("key already present");
        return new PersistentPrioritySearchQueue(
                newWinner,
                kcomp,
                pcomp,
                _count + 1,
                _meta
        );
    }

    // clojure.lang.IPersistentStack

    public Object peek() {
        if (isEmpty())
            return null;
        return new MapEntry(winner.key, winner.priority);
    }

    public PersistentPrioritySearchQueue pop() {
        return new PersistentPrioritySearchQueue(
                secondBest(winner.losers, winner.ubound),
                kcomp,
                pcomp,
                _count - 1,
                _meta
        );
    }

    // clojure.lang.Reversible

    public ISeq rseq() {
        if (_count == 0)
            return null;
        return rtraverse(winner.key, winner.priority, winner.losers);
    }

    // clojure.lang.Seqable

    public ISeq seq() {
        if (_count == 0)
            return null;
        return traverse(winner.key, winner.priority, winner.losers);
    }

    // clojure.lang.Sorted

    public ISeq seq(boolean ascending) {
        if (ascending)
            return seq();
        return rseq();
    }

    public ISeq seqFrom(Object k, boolean ascending) {
        if (_count == 0)
            return null;
        ISeq ret;
        if (ascending)
            ret = traverseFrom(k, winner);
        else
            ret = rtraverseFrom(k, winner);
        return RT.seq(ret);
    }

    public Object entryKey(Object entry) {
        return ((Map.Entry) entry).getKey();
    }

    public Comparator comparator() {
        return kcomp;
    }

    // Iterable

    public Iterator iterator() {
        if (isEmpty()) {
            return ((Iterable) PersistentList.EMPTY).iterator();
        }
        return ((Iterable) seq()).iterator();
    }
}
