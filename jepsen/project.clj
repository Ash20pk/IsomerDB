(defproject isomerdb-jepsen "0.1.0"
  :description "Jepsen tests for IsomerDB: transactional isolation across shards, read-your-writes on projections over SQL and MongoDB, and incrementally maintained projections, under node, network and clock faults"
  :license {:name "Apache License, Version 2.0" :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :main isomerdb.jepsen
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.14"]
                 [com.github.seancorfield/next.jdbc "1.3.1118"]
                 [org.postgresql/postgresql "42.7.7"]
                 [org.mongodb/mongodb-driver-sync "5.2.1"]]
  :jvm-opts ["-Xmx2g" "-Djava.awt.headless=true"])
