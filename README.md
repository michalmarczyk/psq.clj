# psq.clj

Persistent Priority Search Queues in Clojure, based on Ralf Hinze's priority
search pennants (see R. Hinze, *A Simple Implementation Technique for Priority
Search Queues*).

Priority Search Queues are sorted maps that recognize an independent ordering
on their entries' *values* – in this context known as priorities – in addition
to the ordering on keys.

They support the full Clojure sorted map API, with `(r)(sub)seq` respecting key
order, as well as priority-based `peek` and `pop`, priority-order traversals and
highly efficient `(r)(sub)seq`-like key order traversals with an additional `<`
or `<=` constraint on the priorities of the returned entries.

In addition to the functionality presented in the paper, this implementation
supports `nearest` (look up the value at the key closest to and less/greater
than the given key, inclusive or exclusive), `nth` (in key order), `rank` (look
up a key's index in key order), `split` (sub-PSQs less/greater than the given
key, plus the entry at the key if present) and `subrange` (sub-PSQs bounded by
the given keys), all in logarithmic time.

In other words, the abstract data type on offer is a superset of that supported
by [data.avl](https://github.com/clojure/data.avl), with the additional
operations exposing the priority queue aspect of PSQs or blending the sorted map
and priority queue aspects.


## Maturity

**Experimental:**

The public features are expected to work with the versions of Clojure listed in
`project.clj` and a thorough generative test suite using
[test.check](https://github.com/clojure/test.check) and
[collection-check](https://github.com/ztellman/collection-check) is in place,
however both the public API and the implementation strategy may be revised.


## Usage

psq.clj priority search queues are, at a high level, sorted maps that support
certain additional operations. Their printed representation is like that of
regular sorted maps.

All the operations listed above are performed in logarithmic time, with the
exception of `peek` (constant time, extremely fast) and priority-bounded
traversals (`O(r(log n - log r) + r)`, where `r` is the number of entries
actually returned). These exceptions are also noted below.


### The public namespace

There is a single public namespace called `psq.clj`:

```clojure
(require '[psq.clj :as psq])
```


### Factory functions

psq.clj exposes six factory functions. Three of these use Clojure's default
comparator (the one backing `clojure.core/sorted-map`, equivalent to
`clojure.core/compare`):

```clojure
;; The psq.clj counterpart to clojure.core/sorted-map:
(psq/psqueue key priority …)
;= {key priority …}

;; A version of the above that takes keys + priorities in a seqable:
(psq/psqueue* [key priority …])

;; A factory that accepts a map or a seqable of map entries:
(psq/psq {key priority …})
(psq/psq [[key priority] …])
```

The other three are versions of the above that take *two* custom comparators:

 * the first one determines the new PSQ's key order;

 * the second one is used for priorities.

```clojure
(psq/psqueue-by > > key priority …) ; reverse numeric order on keys
                                    ; and priorities

psq/psqueue-by*, psq/psq-by ; like psq/psqueue*, psq/psq, but with
                            ; custom comparators
```


### Regular sorted map API

All operations supported by Clojure's built-in sorted maps are supported by
psq.clj priority search queues: `assoc`, `dissoc`, `conj`, `seq`, `rseq`,
`subseq`, `rsubseq`. Note that `(r)(sub)seq` follow key order:

```clojure
(seq (psq/psqueue 0 10 1 9))
;= ([0 10] [1 9])
```


### Nearest neighbour lookups

Find the entry whose key is nearest to the given key and `<` / `<=` / `>=` / `>`
than the given key (`nil` if no such entry exists, for example if the test is
`>` and the key passed in is `>=` to the greatest key in the PSQ).

```clojure
(psq/nearest (psq/psq {0 1 4 5 9 10}) > 3)
;= [4 5]
```


### `nth`, `rank` in key order

`nth` accesses the entry at the given index in the input PSQ's key order:

```clojure
(nth (psq/psqueue 0 3 6 -3) 0)
;= [0 3]
```

`rank` returns the index of the given key in the input PSQ as a primitive
`long` or `-1` for not found:

```clojure
(psq/rank (psq/psqueue 0 3 6 -3) 6)
;= 1
```


### Splits, subranges

The PSQs returned by the following two operations share structure with the input
PSQs in the common parts, but they do not hold on to any entries outside the
stated range for GC purposes – they are completely independent, first-class
PSQs.

`split` returns a vector of

 1. a fully independent PSQ comprising the entries of the input PSQ to the left
    of the given key,

 2. the entry at the given key, or `nil` if not present,

 3. a fully independent PSQ comprising the entries of the input PSQ to the right
    of the given key.

```clojure
(psq/split (psq/psqueue* (range 10)) 4)
;= [{0 1 2 3} [4 5] {6 7 8 9}]
```

`subrange` is similar to `subseq`, but rather than returning a seq of entries,
it returns a fully independent PSQ comprising the entries of the input PSQ that
fall within the given key range:

```clojure
(psq/subrange (psq/psqueue* (range 10)) >= 4 < 8)
;= {4 5 6 7}
```


### Priority queue API based on values/priorities

psq.clj priority search queues support `clojure.core/peek` and
`clojure.core/pop`. `peek` returns an entry with the minimum priority (NB. there
can be multiple entries with any given priority).

```clojure
;; NB. peek is constant-time and extremely fast
(peek (psq/psqueue 0 3 1 -3))
;= [1 -3]
```

`pop` removes the entry that `peek` would return:

```clojure
(pop (psq/psqueue 0 3 1 -3))
;= {0 3}
```


### Priority-order traversals

PSQs can be traversed in order of non-decreasing priorities:

```clojure
(psq/priority-seq (psq/psqueue 0 3 1 10 2 4 3 8 5 12 6 0))
;= ([6 0] [0 3] [2 4] [3 8] [1 10] [5 12])
```


### Priority-bounded traversals

psq.clj exposes counterparts to `(r)(sub)seq` that take an additional initial
argument interpreted as an upper bound on the priorities of entries that may be
returned.

These operations are performed in time `O(r(log n - log r) + r)`, where `r` is
the number of entries actually returned. In practice, they are much faster than
the equivalent combinations of `(r)(sub)seq` and `filter`; if the input key
range includes a large number of entries only a small number of which satisfies
the priority constraint, the advantage over `r(sub)(seq)` composed with `filter`
can reach several orders of magnitude.

For example, these calls return the entries in the given range (the full PSQ or
the `> 0 <= 5` range) whose priorities are `<=` than the given upper bound of
`20` in the ordering determined by the PSQs' priority comparator:

```clojure
(psq/seq<= (psq/psqueue 0 1 2 5 3 1 4 100 5 3 6 10) 20)
;= ([0 1] [2 5] [3 1] [5 3] [6 10])

(psq/subseq<= (psq/psqueue 0 1 2 5 3 1 4 100 5 3 6 10) 20 > 0 <= 5)
;= ([2 5] [3 1] [5 3])
```

Also available:

 * `rseq<=`, `rsubseq<=` – as above, with the output seqs reversed,

 * `seq<`, `rseq<`, `subseq<`, `rsubseq<` – as above, returning only entries
   with priorities `<` than the given bound.


## Releases and dependency information

This is an experimental library.
[Alpha releases are available from Clojars](https://clojars.org/psq.clj),
however a custom build may currently be necessary for use with versions of
Clojure other than the one used to build the release. (This will be fixed in
a future release.) Follow the link above to discover the current release number.

[Leiningen](http://leiningen.org/) dependency information:

    [psq.clj "${version}"]

[Maven](http://maven.apache.org/) dependency information:

    <dependency>
      <groupId>psq.clj</groupId>
      <artifactId>psq.clj</artifactId>
      <version>${version}</version>
    </dependency>

[Gradle](http://www.gradle.org/) dependency information:

    compile "psq.clj:psq.clj:${version}"


## Clojure code reuse

The implementations of the `static public IPersistentMap create(…)` and
`static public PersistentPrioritySearchQueue create(…)` methods are adapted from
the implementations of the analogous methods in Clojure.

The Clojure source files containing the relevant code carry the following
copyright notice:

    Copyright (c) Rich Hickey. All rights reserved.
    The use and distribution terms for this software are covered by the
    Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
    which can be found in the file epl-v10.html at the root of this distribution.
    By using this software in any fashion, you are agreeing to be bound by
      the terms of this license.
    You must not remove this notice, or any other, from this software.


## Licence

Copyright © 2016 Michał Marczyk

Distributed under the Eclipse Public License version 1.0.
