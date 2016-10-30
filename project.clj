(defproject psq.clj "0.0.2-SNAPSHOT"
  :description "Persistent Priority Search Queues in Clojure"
  :url "https://github.com/michalmarczyk/psq.clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]]
  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :profiles {:dev {:dependencies [[org.clojure/test.check "0.9.0"]
                                  [collection-check "0.1.6"]]
                   :jvm-opts ^:replace ["-XX:+UseG1GC"]}})
