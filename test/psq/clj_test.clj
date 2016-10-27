(ns psq.clj-test
  (:use clojure.test)
  (:require [psq.clj :as psq]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [collection-check :refer [assert-map-like]])
  (:import (clojure.lang MapEntry)))


(def igen gen/int)


(deftest collection-check
  (assert-map-like 1000
                   (psq/psqueue)
                   igen igen {:ordered? true :base (sorted-map)}))


(def psqgen
  (gen/bind
    (gen/sorted-set igen)
    (fn [ks]
      (let [ps (gen/sample igen (count ks))]
        (gen/return (psq/psqueue* (interleave (vec ks) ps)))))))


(defn loser-node-set [^psq.PersistentPrioritySearchQueue$Loser loser]
  (if (nil? loser)
    #{}
    (let [lkey (.-key loser)
          lsplit (.-split loser)
          lentry (MapEntry. lkey (.-priority loser))]
      (conj (loser-node-set (if (<= lkey lsplit)
                              (.-left loser)
                              (.-right loser)))
            lentry))))

(defn loser-set [^psq.PersistentPrioritySearchQueue$Loser loser]
  (if (nil? loser)
    #{}
    (into #{loser}
          (concat (loser-set (.-left loser))
                  (loser-set (.-right loser))))))

(defn loser->entry [^psq.PersistentPrioritySearchQueue$Loser loser]
  (MapEntry. (.-key loser) (.-priority loser)))

(defn satisfies-invariant? [psq]
  (if (empty? psq)
    true
    (let [winner (.-winner ^psq.PersistentPrioritySearchQueue$Winner psq)
          wkey (.-key winner)
          wpriority (.-priority winner)
          wlosers (.-losers winner)
          wubound (.-ubound winner)
          wlset (loser-set wlosers)
          wlentries (map loser->entry wlset)
          wkeyset (conj (set (map key wlentries)) wkey)]
      (and (every? #(<= wpriority (val %)) wlentries)
           (every? (fn [^psq.PersistentPrioritySearchQueue$Loser loser]
                     (every? #(<= (.-priority loser) (val %))
                             (loser-node-set loser)))
                   wlset)
           (<= wkey wubound)
           (every? #(<= (key %) wubound) wlentries)
           (or (== wkey wubound)
               (some #(== % wubound) (map key wlentries)))
           (every? (fn [^psq.PersistentPrioritySearchQueue$Loser loser]
                     (and (every? #(<= (key %) (.-split loser))
                                  (map loser->entry
                                       (loser-set (.-left loser))))
                          (every? #(< (.-split loser) (key %))
                                  (map loser->entry
                                       (loser-set (.-right loser))))))
                   wlset)
           (every? (fn [^psq.PersistentPrioritySearchQueue$Loser loser]
                     (contains? wkeyset (.-split loser)))
                   wlset)
           (== (count wkeyset) (inc (count wlset)))))))

(defspec check-invariant 100
  (prop/for-all [m (gen/not-empty psqgen)]
    (satisfies-invariant? m)))


(defspec check-nth 100
  (prop/for-all [m psqgen]
    (= (sequence m) (map #(nth m %) (range (count m))))))


(defspec check-rank 100
  (prop/for-all [m psqgen
                 k igen]
    (let [r (psq/rank m k)]
      (if (== r -1)
        (not (contains? m k))
        (= k (key (nth m r)))))))


(defspec check-split 100
  (prop/for-all [m psqgen
                 k igen]
    (let [[l e r] (psq/split m k)]
      (and (= m (-> (psq/psqueue)
                    (into l)
                    (conj e)
                    (into r)))
           (every? #(< % k) (keys l))
           (every? #(< k %) (keys r))
           (if (contains? m k)
             (and (= k (key e)))
             (nil? e))
           (satisfies-invariant? l)
           (satisfies-invariant? r)))))


(defspec check-seq 100
  (prop/for-all [m psqgen]
    (let [sm (into (sorted-map) m)]
      (and (= sm m)
           (= (seq sm) (seq m))))))


(defspec check-priority-seq 100
  (prop/for-all [m psqgen]
    (= (map set (partition-by val (psq/priority-seq m)))
       (map set (partition-by val (sort-by val (seq m)))))))


(defn subseq-subrange
  ([psq test limit]
   (into (empty psq) (subseq psq test limit)))
  ([psq start-test start end-test end]
   (into (empty psq) (subseq psq start-test start end-test end))))


(defspec check-subrange 100
  (prop/for-all [m psqgen
                 [start end] (gen/such-that (fn [[l h]] (< l h))
                                            (gen/tuple igen igen)
                                            100)
                 start-test (gen/elements [> >=])
                 end-test (gen/elements [< <=])]
    (let [sub (psq/subrange m start-test start end-test end)]
      (and (satisfies-invariant? sub)
           (= sub (subseq-subrange m start-test start end-test end))))))


(defspec check-single-limit-subrange 100
  (prop/for-all [m psqgen
                 limit igen
                 test (gen/elements [< <= >= >])]
    (let [sub (psq/subrange m test limit)]
      (and (satisfies-invariant? sub)
           (= sub (subseq-subrange m test limit))))))


(defspec check-singleton-subrange 100
  (prop/for-all [m psqgen
                 k igen]
    (let [sub (psq/subrange m >= k <= k)]
      (and (satisfies-invariant? sub)
           (= sub (subseq-subrange m >= k <= k))))))


(defn filter-subseq<=
  ([psq ubound test limit]
   (filter #(<= (val %) ubound)
           (subseq psq test limit)))
  ([psq ubound start-test start end-test end]
   (filter #(<= (val %) ubound)
           (subseq psq start-test start end-test end))))


(defn filter-rsubseq<=
  ([psq ubound test limit]
   (filter #(<= (val %) ubound)
           (rsubseq psq test limit)))
  ([psq ubound start-test start end-test end]
   (filter #(<= (val %) ubound)
           (rsubseq psq start-test start end-test end))))


(defspec check-subseq<= 100
  (prop/for-all [m psqgen
                 [start end] (gen/such-that (fn [[l h]] (< l h))
                                            (gen/tuple igen igen)
                                            100)
                 ubound igen
                 start-test (gen/elements [> >=])
                 end-test (gen/elements [< <=])]
    (= (psq/subseq<= m ubound start-test start end-test end)
       (filter-subseq<= m ubound start-test start end-test end))))


(defspec check-rsubseq<= 100
  (prop/for-all [m psqgen
                 [start end] (gen/such-that (fn [[l h]] (< l h))
                                            (gen/tuple igen igen)
                                            100)
                 ubound igen
                 start-test (gen/elements [> >=])
                 end-test (gen/elements [< <=])]
    (= (psq/rsubseq<= m ubound start-test start end-test end)
       (filter-rsubseq<= m ubound start-test start end-test end))))


(defspec check-single-limit-subseq<= 100
  (prop/for-all [m psqgen
                 ubound igen
                 test (gen/elements [< <= >= >])
                 limit igen]
    (= (psq/subseq<= m ubound test limit)
       (filter-subseq<= m ubound test limit))))


(defspec check-single-limit-rsubseq<= 100
  (prop/for-all [m psqgen
                 ubound igen
                 test (gen/elements [< <= >= >])
                 limit igen]
    (= (psq/rsubseq<= m ubound test limit)
       (filter-rsubseq<= m ubound test limit))))


(defspec check-seq<= 100
  (prop/for-all [m psqgen
                 ubound igen]
    (= (psq/seq<= m ubound)
       (filter #(<= (val %) ubound) (seq m)))))


(defspec check-rseq<= 100
  (prop/for-all [m psqgen
                 ubound igen]
    (= (psq/rseq<= m ubound)
       (filter #(<= (val %) ubound) (rseq m)))))


(defspec check-factories 100
  (prop/for-all [xs (gen/such-that #(even? (count %))
                                   (gen/vector igen)
                                   100)]
    (= (apply sorted-map xs)
       (apply psq/psqueue xs)
       (psq/psqueue* xs)
       (psq/psq (map vec (partition 2 xs))))))


(defn subseq-nearest [psq test key]
  (let [seqfn (cond
                (#{< <=} test) rsubseq
                (#{> >=} test) subseq
                :else          (throw
                                 (ex-info "Incorrect test in subseq-nearest" {})))]
    (first (seqfn psq test key))))


(defspec check-nearest 100
  (prop/for-all [m psqgen
                 test (gen/elements [< <= >= >])
                 key gen/int]
    (= (subseq-nearest m test key) (psq/nearest m test key))))
