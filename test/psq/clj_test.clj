(ns psq.clj-test
  (:use clojure.test)
  (:require [psq.clj :as psq]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [collection-check :as cc])
  (:import (clojure.lang MapEntry)
           (java.util Comparator)))


(def igen gen/int)


(deftest collection-check
  (is (cc/assert-map-like 1000
                          (psq/psqueue)
                          igen igen {:ordered? true :base (sorted-map)})))


(deftest collection-check-by
  (is (cc/assert-map-like 1000
                          (psq/psqueue-by > >)
                          igen igen {:ordered? true :base (sorted-map-by >)})))


(def psqgen
  (gen/bind
    (gen/sorted-set igen)
    (fn [init-ks]
      (let [init-ps (gen/sample igen (count init-ks))
            init-keypriorities (interleave init-ks init-ps)]
        (gen/bind
          (#'cc/gen-map-actions igen igen false true)
          (fn [actions]
            (gen/return
              (first (#'cc/build-collections
                       (psq/psqueue* init-keypriorities)
                       (apply sorted-map init-keypriorities)
                       false
                       actions)))))))))


(defn psqgen-by [kcomp pcomp]
  (gen/bind
    (gen/sorted-set igen)
    (fn [init-ks]
      (let [init-ps (gen/sample igen (count init-ks))
            init-keypriorities (interleave init-ks init-ps)]
        (gen/bind
          (#'cc/gen-map-actions igen igen false true)
          (fn [actions]
            (gen/return
              (first (#'cc/build-collections
                       (psq/psqueue-by* kcomp pcomp init-keypriorities)
                       (apply sorted-map-by kcomp init-keypriorities)
                       false
                       actions)))))))))


(defn loser-node-set
  [^Comparator kcomp ^psq.PersistentPrioritySearchQueue$Loser loser]
  (if (nil? loser)
    #{}
    (let [lkey (.-key loser)
          lsplit (.-split loser)
          lentry (MapEntry. lkey (.-priority loser))]
      (conj (loser-node-set kcomp
                            (if (<= (.compare kcomp lkey lsplit) 0)
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

(defn satisfies-invariant? [^psq.PersistentPrioritySearchQueue psq]
  (if (empty? psq)
    true
    (let [kcomp (.-kcomp psq)
          pcomp (.-pcomp psq)
          k< #(neg? (.compare kcomp %1 %2))
          k<= #(<= (.compare kcomp %1 %2) 0)
          p<= #(<= (.compare pcomp %1 %2) 0)
          winner (.-winner psq)
          wkey (.-key winner)
          wpriority (.-priority winner)
          wlosers (.-losers winner)
          wubound (.-ubound winner)
          wlset (loser-set wlosers)
          wlentries (map loser->entry wlset)
          wkeyset (conj (set (map key wlentries)) wkey)]
      (and (every? #(p<= wpriority (val %)) wlentries)
           (every? (fn [^psq.PersistentPrioritySearchQueue$Loser loser]
                     (every? #(p<= (.-priority loser) (val %))
                             (loser-node-set kcomp loser)))
                   wlset)
           (k<= wkey wubound)
           (every? #(k<= (key %) wubound) wlentries)
           (or (== wkey wubound)
               (some #(== % wubound) (map key wlentries)))
           (every? (fn [^psq.PersistentPrioritySearchQueue$Loser loser]
                     (and (every? #(k<= (key %) (.-split loser))
                                  (map loser->entry
                                       (loser-set (.-left loser))))
                          (every? #(k< (.-split loser) (key %))
                                  (map loser->entry
                                       (loser-set (.-right loser))))))
                   wlset)
           (every? (fn [^psq.PersistentPrioritySearchQueue$Loser loser]
                     (contains? wkeyset (.-split loser)))
                   wlset)
           (== (count wkeyset) (inc (count wlset)))))))


(defspec check-invariant 100
  (prop/for-all [m psqgen]
    (satisfies-invariant? m)))


(defspec check-invariant-by 100
  (prop/for-all [m (psqgen-by > >)]
    (satisfies-invariant? m)))


(defspec check-invariant-collection-check 100
  (prop/for-all [actions (#'cc/gen-map-actions igen igen false true)]
    (let [[psq] (#'cc/build-collections
                  (psq/psqueue) (sorted-map) false actions)]
      (satisfies-invariant? psq))))


(defspec check-invariant-collection-check-by 100
  (prop/for-all [actions (#'cc/gen-map-actions igen igen false true)]
    (let [[psq] (#'cc/build-collections
                  (psq/psqueue-by > >) (sorted-map-by >) false actions)]
      (satisfies-invariant? psq))))


(defspec check-contains? 100
  (prop/for-all [m (gen/map igen igen)
                 k igen]
    (let [psq (psq/psq m)]
      (= (contains? m k)
         (contains? psq k)))))


(defspec check-contains?-by 100
  (prop/for-all [m (gen/map igen igen)
                 k igen]
    (let [psq (psq/psq-by > > m)]
      (= (contains? m k)
         (contains? psq k)))))


(defspec check-get 100
  (prop/for-all [m (gen/map igen igen)
                 k igen]
    (let [psq (psq/psq m)]
      (and (= (get m k)
              (get psq k))
           (= (get m k ::not-found)
              (get psq k ::not-found))))))


(defspec check-get-by 100
  (prop/for-all [m (gen/map igen igen)
                 k igen]
    (let [psq (psq/psq-by > > m)]
      (and (= (get m k)
              (get psq k))
           (= (get m k ::not-found)
              (get psq k ::not-found))))))


(defspec check-nth 100
  (prop/for-all [m psqgen]
    (= (sequence m) (map #(nth m %) (range (count m))))))


(defspec check-nth-by 100
  (prop/for-all [m (psqgen-by > >)]
    (= (sequence m) (map #(nth m %) (range (count m))))))


(defspec check-rank 100
  (prop/for-all [m psqgen
                 k igen]
    (let [r (psq/rank m k)]
      (if (== r -1)
        (not (contains? m k))
        (= k (key (nth m r)))))))


(defspec check-rank-by 100
  (prop/for-all [m (psqgen-by > >)
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


(defspec check-split-by 100
  (prop/for-all [m (psqgen-by > >)
                 k igen]
    (let [[l e r] (psq/split m k)]
      (and (= m (-> (psq/psqueue-by > >)
                    (into l)
                    (conj e)
                    (into r)))
           (every? #(> % k) (keys l))
           (every? #(> k %) (keys r))
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


(defspec check-seq-by 100
  (prop/for-all [m (psqgen-by > >)]
    (let [sm (into (sorted-map-by >) m)]
      (and (= sm m)
           (= (seq sm) (seq m))))))


(defspec check-priority-seq 100
  (prop/for-all [m psqgen]
    (= (map set (partition-by val (psq/priority-seq m)))
       (map set (partition-by val (sort-by val (seq m)))))))


(defn peek-pop-priority-seq [psq]
  (if (seq psq)
    (cons (peek psq)
          (lazy-seq (peek-pop-priority-seq (pop psq))))))


(defspec check-peek-pop 100
  (prop/for-all [m psqgen]
    (= (psq/priority-seq m)
       (peek-pop-priority-seq m))))


(defspec check-priority-seq-by 100
  (prop/for-all [m (psqgen-by > >)]
    (= (map set (partition-by val (psq/priority-seq m)))
       (map set (partition-by val (sort-by val > (seq m)))))))


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


(defspec check-subrange-by 100
  (prop/for-all [m (psqgen-by > >)
                 [start end] (gen/such-that (fn [[l h]] (> l h))
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


(defspec check-single-limit-subrange-by 100
  (prop/for-all [m (psqgen-by > >)
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


(defspec check-singleton-subrange-by 100
  (prop/for-all [m (psqgen-by > >)
                 k igen]
    (let [sub (psq/subrange m >= k <= k)]
      (and (satisfies-invariant? sub)
           (= sub (subseq-subrange m >= k <= k))))))


(defn filter-seq<
  [psq ubound]
  (filter #(< (val %) ubound)
          (seq psq)))


(defn filter-seq<=
  [psq ubound]
  (filter #(<= (val %) ubound)
          (seq psq)))


(defn filter-rseq<
  [psq ubound]
  (filter #(< (val %) ubound)
          (rseq psq)))


(defn filter-rseq<=
  [psq ubound]
  (filter #(<= (val %) ubound)
          (rseq psq)))


(defn filter-subseq<
  ([psq ubound test limit]
   (filter #(< (val %) ubound)
           (subseq psq test limit)))
  ([psq ubound start-test start end-test end]
   (filter #(< (val %) ubound)
           (subseq psq start-test start end-test end))))


(defn filter-subseq<=
  ([psq ubound test limit]
   (filter #(<= (val %) ubound)
           (subseq psq test limit)))
  ([psq ubound start-test start end-test end]
   (filter #(<= (val %) ubound)
           (subseq psq start-test start end-test end))))


(defn filter-rsubseq<
  ([psq ubound test limit]
   (filter #(< (val %) ubound)
           (rsubseq psq test limit)))
  ([psq ubound start-test start end-test end]
   (filter #(< (val %) ubound)
           (rsubseq psq start-test start end-test end))))


(defn filter-rsubseq<=
  ([psq ubound test limit]
   (filter #(<= (val %) ubound)
           (rsubseq psq test limit)))
  ([psq ubound start-test start end-test end]
   (filter #(<= (val %) ubound)
           (rsubseq psq start-test start end-test end))))


(defspec check-subseq< 100
  (prop/for-all [m psqgen
                 [start end] (gen/such-that (fn [[l h]] (< l h))
                                            (gen/tuple igen igen)
                                            100)
                 ubound igen
                 start-test (gen/elements [> >=])
                 end-test (gen/elements [< <=])]
    (= (psq/subseq< m ubound start-test start end-test end)
       (filter-subseq< m ubound start-test start end-test end))))


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


(defspec check-rsubseq< 100
  (prop/for-all [m psqgen
                 [start end] (gen/such-that (fn [[l h]] (< l h))
                                            (gen/tuple igen igen)
                                            100)
                 ubound igen
                 start-test (gen/elements [> >=])
                 end-test (gen/elements [< <=])]
    (= (psq/rsubseq< m ubound start-test start end-test end)
       (filter-rsubseq< m ubound start-test start end-test end))))


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


(defspec check-single-limit-subseq< 100
  (prop/for-all [m psqgen
                 ubound igen
                 test (gen/elements [< <= >= >])
                 limit igen]
    (= (psq/subseq< m ubound test limit)
       (filter-subseq< m ubound test limit))))


(defspec check-single-limit-subseq<= 100
  (prop/for-all [m psqgen
                 ubound igen
                 test (gen/elements [< <= >= >])
                 limit igen]
    (= (psq/subseq<= m ubound test limit)
       (filter-subseq<= m ubound test limit))))


(defspec check-single-limit-rsubseq< 100
  (prop/for-all [m psqgen
                 ubound igen
                 test (gen/elements [< <= >= >])
                 limit igen]
    (= (psq/rsubseq< m ubound test limit)
       (filter-rsubseq< m ubound test limit))))


(defspec check-single-limit-rsubseq<= 100
  (prop/for-all [m psqgen
                 ubound igen
                 test (gen/elements [< <= >= >])
                 limit igen]
    (= (psq/rsubseq<= m ubound test limit)
       (filter-rsubseq<= m ubound test limit))))


(defspec check-seq< 100
  (prop/for-all [m psqgen
                 ubound igen]
    (= (psq/seq<= m ubound)
       (filter-seq<= m ubound))))


(defspec check-seq<= 100
  (prop/for-all [m psqgen
                 ubound igen]
    (= (psq/seq<= m ubound)
       (filter-seq<= m ubound))))


(defspec check-rseq< 100
  (prop/for-all [m psqgen
                 ubound igen]
    (= (psq/rseq<= m ubound)
       (filter-rseq<= m ubound))))


(defspec check-rseq<= 100
  (prop/for-all [m psqgen
                 ubound igen]
    (= (psq/rseq<= m ubound)
       (filter-rseq<= m ubound))))


(defn filter-seq<-by
  [psq ^Comparator pcomp ubound]
  (filter #(< (.compare pcomp (val %) ubound) 0) (seq psq)))


(defn filter-seq<=-by
  [psq ^Comparator pcomp ubound]
  (filter #(<= (.compare pcomp (val %) ubound) 0) (seq psq)))


(defn filter-rseq<-by
  [psq ^Comparator pcomp ubound]
  (filter #(< (.compare pcomp (val %) ubound) 0) (rseq psq)))


(defn filter-rseq<=-by
  [psq ^Comparator pcomp ubound]
  (filter #(<= (.compare pcomp (val %) ubound) 0) (rseq psq)))


(defn filter-subseq<-by
  ([psq ^Comparator pcomp ubound test limit]
   (filter #(< (.compare pcomp (val %) ubound) 0)
           (subseq psq test limit)))
  ([psq ^Comparator pcomp ubound start-test start end-test end]
   (filter #(< (.compare pcomp (val %) ubound) 0)
           (subseq psq start-test start end-test end))))


(defn filter-subseq<=-by
  ([psq ^Comparator pcomp ubound test limit]
   (filter #(<= (.compare pcomp (val %) ubound) 0)
           (subseq psq test limit)))
  ([psq ^Comparator pcomp ubound start-test start end-test end]
   (filter #(<= (.compare pcomp (val %) ubound) 0)
           (subseq psq start-test start end-test end))))


(defn filter-rsubseq<-by
  ([psq ^Comparator pcomp ubound test limit]
   (filter #(< (.compare pcomp (val %) ubound) 0)
           (rsubseq psq test limit)))
  ([psq ^Comparator pcomp ubound start-test start end-test end]
   (filter #(< (.compare pcomp (val %) ubound) 0)
           (rsubseq psq start-test start end-test end))))


(defn filter-rsubseq<=-by
  ([psq ^Comparator pcomp ubound test limit]
   (filter #(<= (.compare pcomp (val %) ubound) 0)
           (rsubseq psq test limit)))
  ([psq ^Comparator pcomp ubound start-test start end-test end]
   (filter #(<= (.compare pcomp (val %) ubound) 0)
           (rsubseq psq start-test start end-test end))))


(defspec check-subseq<-by 100
  (prop/for-all [m (psqgen-by > >)
                 [start end] (gen/such-that (fn [[l h]] (> l h))
                                            (gen/tuple igen igen)
                                            100)
                 ubound igen
                 start-test (gen/elements [> >=])
                 end-test (gen/elements [< <=])]
    (= (psq/subseq< m ubound start-test start end-test end)
       (filter-subseq<-by m > ubound start-test start end-test end))))


(defspec check-subseq<=-by 100
  (prop/for-all [m (psqgen-by > >)
                 [start end] (gen/such-that (fn [[l h]] (> l h))
                                            (gen/tuple igen igen)
                                            100)
                 ubound igen
                 start-test (gen/elements [> >=])
                 end-test (gen/elements [< <=])]
    (= (psq/subseq<= m ubound start-test start end-test end)
       (filter-subseq<=-by m > ubound start-test start end-test end))))


(defspec check-rsubseq<-by 100
  (prop/for-all [m (psqgen-by > >)
                 [start end] (gen/such-that (fn [[l h]] (> l h))
                                            (gen/tuple igen igen)
                                            100)
                 ubound igen
                 start-test (gen/elements [> >=])
                 end-test (gen/elements [< <=])]
    (= (psq/rsubseq< m ubound start-test start end-test end)
       (filter-rsubseq<-by m > ubound start-test start end-test end))))


(defspec check-rsubseq<=-by 100
  (prop/for-all [m (psqgen-by > >)
                 [start end] (gen/such-that (fn [[l h]] (> l h))
                                            (gen/tuple igen igen)
                                            100)
                 ubound igen
                 start-test (gen/elements [> >=])
                 end-test (gen/elements [< <=])]
    (= (psq/rsubseq<= m ubound start-test start end-test end)
       (filter-rsubseq<=-by m > ubound start-test start end-test end))))


(defspec check-single-limit-subseq<-by 100
  (prop/for-all [m (psqgen-by > >)
                 ubound igen
                 test (gen/elements [< <= >= >])
                 limit igen]
    (= (psq/subseq< m ubound test limit)
       (filter-subseq<-by m > ubound test limit))))


(defspec check-single-limit-subseq<=-by 100
  (prop/for-all [m (psqgen-by > >)
                 ubound igen
                 test (gen/elements [< <= >= >])
                 limit igen]
    (= (psq/subseq<= m ubound test limit)
       (filter-subseq<=-by m > ubound test limit))))


(defspec check-single-limit-rsubseq<-by 100
  (prop/for-all [m (psqgen-by > >)
                 ubound igen
                 test (gen/elements [< <= >= >])
                 limit igen]
    (= (psq/rsubseq< m ubound test limit)
       (filter-rsubseq<-by m > ubound test limit))))


(defspec check-single-limit-rsubseq<=-by 100
  (prop/for-all [m (psqgen-by > >)
                 ubound igen
                 test (gen/elements [< <= >= >])
                 limit igen]
    (= (psq/rsubseq<= m ubound test limit)
       (filter-rsubseq<=-by m > ubound test limit))))


(defspec check-seq<-by 100
  (prop/for-all [m (psqgen-by > >)
                 ubound igen]
    (= (psq/seq< m ubound)
       (filter-seq<-by m > ubound))))


(defspec check-seq<=-by 100
  (prop/for-all [m (psqgen-by > >)
                 ubound igen]
    (= (psq/seq<= m ubound)
       (filter-seq<=-by m > ubound))))


(defspec check-rseq<-by 100
  (prop/for-all [m (psqgen-by > >)
                 ubound igen]
    (= (psq/rseq< m ubound)
       (filter-rseq<-by m > ubound))))


(defspec check-rseq<=-by 100
  (prop/for-all [m (psqgen-by > >)
                 ubound igen]
    (= (psq/rseq<= m ubound)
       (filter-rseq<=-by m > ubound))))


(defspec check-factories 100
  (prop/for-all [xs (gen/such-that #(even? (count %))
                                   (gen/vector igen)
                                   100)]
    (= (apply sorted-map xs)
       (apply psq/psqueue xs)
       (apply psq/psqueue-by > > xs)
       (psq/psqueue* xs)
       (psq/psq (map vec (partition 2 xs)))
       (psq/psq (apply hash-map xs)))))


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


(defspec check-nearest-by 100
  (prop/for-all [m (psqgen-by > >)
                 test (gen/elements [< <= >= >])
                 key gen/int]
    (= (subseq-nearest m test key) (psq/nearest m test key))))
