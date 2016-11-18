(ns psq.clj

  "Persistent Priority Search Queues implemented using Ralf Hinze's priority
  search pennants, see R. Hinze, A Simple Implementation Technique
  for Priority Search Queues.

  In addition to the functionality presented in the paper, this implementation
  supports nearest (look up the value at the key closest to and less/greater
  than the given key, inclusive or exclusive), nth (in key order), rank (look
  up a key's index in key order), split (sub-PSQs less/greater than the given
  key, plus the entry at the key if present) and subrange (sub-PSQs bounded by
  the given keys), all in logarithmic time."

  {:author "Michał Marczyk"}

  (:import (psq IPrioritySearchQueue PersistentPrioritySearchQueue)
           (java.util Comparator)))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn psq
  "Returns a new priority search queue containing the contents of coll, which
  must be a collection of map entries or doubleton vectors."
  [coll]
  (reduce conj PersistentPrioritySearchQueue/EMPTY coll))


(defn psqueue*
  "keypriority => key priority
  Returns a new priority search queue with supplied mappings. If any keys are
  equal, they are handled as if by repeated uses of assoc. NB. this function
  takes a seqable of keys and priorities; see psqueue for a variant taking
  varargs."
  [keypriorities]
  (PersistentPrioritySearchQueue/create (seq keypriorities)))


(defn psqueue
  "keypriority => key priority
  Returns a new priority search queue with supplied mappings. If any keys are
  equal, they are handled as if by repeated uses of assoc."
  [& keypriorities]
  (psqueue* keypriorities))


(defn psqueue-by*
  "keypriority => key priority
  Returns a new priority search queue with supplied mappings, using the
  supplied comparators. If any keys are equal, they are handled as if by
  repeated uses of assoc. NB. this function takes a seqable of keys and
  priorities; see psqueue-by for a variant taking varargs."
  [key-comparator priority-comparator keypriorities]
  (PersistentPrioritySearchQueue/create
    ^Comparator key-comparator
    ^Comparator priority-comparator
    (seq keypriorities)))


(defn psqueue-by
  "keypriority => key priority
  Returns a new priority search queue with supplied mappings, using the
  supplied comparators. If any keys are equal, they are handled as if by
  repeated uses of assoc."
  [key-comparator priority-comparator & keypriorities]
  (psqueue-by* key-comparator priority-comparator keypriorities))


(defn psq-by
  "Returns a new priority search queue using the supplied comparators and
  containing the contents of coll, which must be a collection of map entries or
  doubleton vectors."
  [key-comparator priority-comparator coll]
  (reduce conj (psqueue-by key-comparator priority-comparator) coll))


(defn priority-seq
  "Returns a seq of entries of the given PSQ in ascending order of
  priority. Entries with equal priorities may be returned in arbitrary
  order."
  [psq]
  (.prioritySeq ^PersistentPrioritySearchQueue psq))


(defn rank
  "Returns the index of the given key in the given PSQ in key order, or -1 if
  not present."
  [psq key]
  (.rank ^PersistentPrioritySearchQueue psq key))


(defn nearest
  "Equivalent to, but more efficient than, (first (subseq* coll test x)),
  where subseq* is clojure.core/subseq for test in #{>, >=} and
  clojure.core/rsubseq for test in #{<, <=}."
  [psq test key]
  (let [psq ^PersistentPrioritySearchQueue psq]
    (condp identical? test
      < (.nearestLeft psq key false)
      <= (.nearestLeft psq key true)
      >= (.nearestRight psq key true)
      > (.nearestRight psq key false)
      (throw
        (ex-info "The test argument to nearest must be one of <, <=, >=, >" {})))))


(defn split
  "Returns

    [left entry right].

  left and right are PSQs comprising the entries in the given PSQ whose keys
  are, respectively, < and > the given split key in the given PSQ's key
  ordering.

  entry is the entry at the split key if present in the given PSQ, otherwise
  nil."
  [psq key]
  (.split ^PersistentPrioritySearchQueue psq key))


(defn subrange
  "Returns a PSQ comprising the entries of the given PSQ between start and end
  (in the sense determined by the given PSQ's comparator) in logarithmic time.
  Whether the endpoints are themselves included in the returned collection
  depends on the provided tests; start-test must be either > or >=, end-test
  must be either < or <=.

  When passed a single test and limit, subrange infers the other end
  of the range from the test: > / >= mean to include items up to the
  end of psq, < / <= mean to include items taken from the beginning
  of psq.

  (subrange psq >= start <= end) is equivalent to, but more efficient
  than, (into (empty psq) (subseq coll >= start <= end))."
  ([psq test limit]
   (cond
     (zero? (count psq))
     psq

     (#{> >=} test)
     (let [k (key (nth psq (dec (count psq))))]
       (if (pos? (.compare (.comparator ^clojure.lang.Sorted psq)
                           limit k))
         (empty psq)
         (subrange psq test limit <= k)))

     :else
     (let [k (key (nth psq 0))]
       (if (neg? (.compare (.comparator ^clojure.lang.Sorted psq)
                           limit k))
         (empty psq)
         (subrange psq >= k test limit)))))
  ([psq start-test start end-test end]
   (if (zero? (count psq))
     psq
     (let [comp (.comparator ^clojure.lang.Sorted psq)]
       (if (pos? (.compare comp start end))
         (throw (IndexOutOfBoundsException.
                  "start greater than end in subrange"))
         (let [l (first (subseq psq start-test start))
               h (first (rsubseq psq end-test end))]
           (if (and l h)
             (let [lk (key l)
                   hk (key h)]
               (if (neg? (.compare comp hk lk))
                 (empty psq)
                 (let [[_ low-e r] (split psq lk)
                       [l high-e _] (split r hk)]
                   (cond-> (or l (empty psq))
                     low-e (conj low-e)
                     high-e (conj high-e)))))
             (empty psq))))))))


(defn ^:private at-most
  ([psq ubound]
   (.atMost ^IPrioritySearchQueue psq ubound))
  ([psq low high ubound]
   (.atMostRange ^IPrioritySearchQueue psq low high ubound)))


(defn ^:private below
  ([psq ubound]
   (.below ^IPrioritySearchQueue psq ubound))
  ([psq low high ubound]
   (.belowRange ^IPrioritySearchQueue psq low high ubound)))


(defn seq<
  "Like seq, but only returns entries with priorities < than the given ubound
  in the ordering determined by psq's priority comparator. This is more
  efficient than using filter."
  [psq ubound]
  (below psq ubound))


(defn seq<=
  "Like seq, but only returns entries with priorities <= than the given ubound
  in the ordering determined by psq's priority comparator. This is more
  efficient than using filter."
  [psq ubound]
  (at-most psq ubound))


(defn subseq<
  "Like subseq, but only returns entries with priorities < than the given
  ubound in the ordering determined by psq's priority comparator. This is more
  efficient than using subseq and filter."
  ([psq ubound test limit]
   (seq< (subrange psq test limit) ubound))
  ([psq ubound start-test start end-test end]
   (seq< (subrange psq start-test start end-test end) ubound)))


(defn subseq<=
  "Like subseq, but only returns entries with priorities <= than the given
  ubound in the ordering determined by psq's priority comparator. This is more
  efficient than using subseq and filter."
  ([psq ubound test limit]
   (seq<= (subrange psq test limit) ubound))
  ([psq ubound start-test start end-test end]
   (seq<= (subrange psq start-test start end-test end) ubound)))


(defn ^:private reverse-at-most
  ([psq ubound]
   (.reverseAtMost ^IPrioritySearchQueue psq ubound))
  ([psq low high ubound]
   (.reverseAtMostRange ^IPrioritySearchQueue psq low high ubound)))


(defn ^:private reverse-below
  ([psq ubound]
   (.reverseBelow ^IPrioritySearchQueue psq ubound))
  ([psq low high ubound]
   (.reverseBelowRange ^IPrioritySearchQueue psq low high ubound)))


(defn rseq<
  "Like rseq, but only returns entries with priorities < than the given ubound
  in the ordering determined by psq's priority comparator. This is more
  efficient than using rseq and filter."
  [psq ubound]
  (reverse-below psq ubound))


(defn rseq<=
  "Like rseq, but only returns entries with priorities <= than the given
  ubound in the ordering determined by psq's priority comparator. This is more
  efficient than using rseq and filter."
  [psq ubound]
  (reverse-at-most psq ubound))


(defn rsubseq<
  "Like rsubseq, but only returns entries with priorities < than the given
  ubound in the ordering determined by psq's priority comparator. This is more
  efficient than using rsubseq and filter."
  ([psq ubound test limit]
   (let [sub (subrange psq test limit)]
     (reverse-below sub ubound)))
  ([psq ubound start-test start end-test end]
   (let [sub (subrange psq start-test start end-test end)]
     (reverse-below sub ubound))))


(defn rsubseq<=
  "Like rsubseq, but only returns entries with priorities <= than the given
  ubound in the ordering determined by psq's priority comparator. This is more
  efficient than using rsubseq and filter."
  ([psq ubound test limit]
   (let [sub (subrange psq test limit)]
     (reverse-at-most sub ubound)))
  ([psq ubound start-test start end-test end]
   (let [sub (subrange psq start-test start end-test end)]
     (reverse-at-most sub ubound))))
