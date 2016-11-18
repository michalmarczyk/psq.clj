# psq.clj

Persistent Priority Search Queues in Clojure, based on Ralf Hinze's priority
search pennants (see R. Hinze, *A Simple Implementation Technique for Priority
Search Queues*).

In addition to the functionality presented in the paper, this implementation
supports nearest (look up the value at the key closest to and less/greater than
the given key, inclusive or exclusive), nth (in key order), rank (look up
a key's index in key order), split (sub-PSQs less/greater than the given key,
plus the entry at the key if present) and subrange (sub-PSQs bounded by
the given keys), all in logarithmic time.


## Usage

There is a single public namespace called `psq.clj`. See the included docstrings
for details.


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
