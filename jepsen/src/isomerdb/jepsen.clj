(ns isomerdb.jepsen
  "Jepsen tests for IsomerDB, run against the docker-compose cluster (a coordinator and two Citus
  workers) while a nemesis pauses, kills and partitions the workers.

  Workloads:

    append  Elle list-append over a table distributed across the nodes: every transaction appends to
            and reads several keys, so most are distributed (two-phase commit). Checked for the
            anomalies READ COMMITTED forbids (G0, G1a, G1b, G1c), and for lost or phantom appends.

    ryw     Read your writes through a projection: a process increments a counter, takes the write's
            token, and reads the counter's document projection with SET isomer.read_after = token.
            A read that answers must show the increment or a later one; the alternative it is allowed
            is an explicit \"not yet\" (IS001). At the end, every acknowledged increment must be in
            the table, and the projection must equal recomputation."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [os :as os]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import (java.sql SQLException)))

;; ── connections ─────────────────────────────────────────────────────────────

(defn datasource [test]
  (jdbc/get-datasource
    {:jdbcUrl (str "jdbc:postgresql://" (:host test) ":5432/postgres"
                   "?user=postgres&password=" (:pg-password test)
                   "&connectTimeout=5&socketTimeout=15&tcpKeepAlive=true")}))

(defn sql-state [^Throwable e]
  (loop [e e]
    (cond (nil? e)                  nil
          (instance? SQLException e) (or (.getSQLState ^SQLException e) "")
          :else                      (recur (.getCause e)))))

(defn outcome
  "How an exception leaves an operation: :fail when it certainly did not take effect, :info when it
  might have (the connection broke after the statement was sent)."
  [op e writes?]
  (let [state (sql-state e)
        msg   (or (.getMessage ^Throwable e) "")]
    (cond
      (= state "IS001")                                   (assoc op :type :fail :error :not-yet)
      (#{"40001" "40P01"} state)                          (assoc op :type :fail :error :conflict)
      (and (not writes?) (some? state))                   (assoc op :type :fail :error [:read state msg])
      (and state (str/starts-with? state "08"))           (assoc op :type :info :error [:connection msg])
      (some? state)                                       (assoc op :type :fail :error [state msg])
      :else                                               (assoc op :type :info :error [:exception msg]))))

;; Jepsen calls setup! once per client, concurrently; the schema is made once per test run.
(def setup-done (atom #{}))

(defn once! [what f]
  (locking setup-done
    (when-not (@setup-done what)
      (f)
      (swap! setup-done conj what))))

;; ── the append workload ─────────────────────────────────────────────────────

(defn parse-list [v] (if (str/blank? v) [] (mapv #(Long/parseLong %) (str/split v #","))))

(defrecord AppendClient [ds conn]
  client/Client
  (open! [this test _node]
    (let [ds (datasource test)]
      (assoc this :ds ds :conn (jdbc/get-connection ds))))
  (setup! [_ test]
    (once! :append #(with-open [c (jdbc/get-connection (datasource test))]
      (jdbc/execute! c ["DROP SCHEMA IF EXISTS jp_append CASCADE"])
      (jdbc/execute! c ["CREATE SCHEMA jp_append"])
      (jdbc/execute! c ["CREATE TABLE jp_append.lists (k bigint PRIMARY KEY, v text NOT NULL)"])
      (jdbc/execute! c ["SELECT isomer.distribute(ARRAY['jp_append.lists']::regclass[], 'k', 8)"]))))
  (invoke! [_ _test op]
    (let [txn    (:value op)
          writes (some #(= :append (first %)) txn)]
      (try
        (jdbc/with-transaction [tx conn {:isolation :read-committed}]
          (assoc op :type :ok
                 :value (mapv (fn [[f k v]]
                                (case f
                                  :r      [f k (some-> (jdbc/execute-one! tx ["SELECT v FROM jp_append.lists WHERE k = ?" k]
                                                                          {:builder-fn rs/as-unqualified-maps})
                                                       :v parse-list)]
                                  :append (do (jdbc/execute! tx [(str "INSERT INTO jp_append.lists AS l (k, v) VALUES (?, ?) "
                                                                      "ON CONFLICT (k) DO UPDATE SET v = l.v || ',' || EXCLUDED.v")
                                                                 k (str v)])
                                              [f k v])))
                              txn)))
        (catch Exception e (outcome op e writes)))))
  (teardown! [_ _test])
  (close! [_ _test] (some-> conn .close)))

;; ── the read-your-writes workload ───────────────────────────────────────────

(def ryw-keys 20)

(defrecord RywClient [ds conn]
  client/Client
  (open! [this test _node]
    (let [ds (datasource test)]
      (assoc this :ds ds :conn (jdbc/get-connection ds))))
  (setup! [_ test]
    (once! :ryw #(with-open [c (jdbc/get-connection (datasource test))]
      (jdbc/execute! c ["SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name = 'jp_counter'"])
      (jdbc/execute! c ["DROP SCHEMA IF EXISTS jp_ryw CASCADE"])
      (jdbc/execute! c ["CREATE SCHEMA jp_ryw"])
      (jdbc/execute! c ["CREATE TABLE jp_ryw.counters (k bigint PRIMARY KEY, n bigint NOT NULL)"])
      (jdbc/execute! c ["SELECT isomer.distribute(ARRAY['jp_ryw.counters']::regclass[], 'k', 8)"])
      (jdbc/execute! c [(str "INSERT INTO jp_ryw.counters SELECT g, 0 FROM generate_series(0, " (dec ryw-keys) ") g")])
      (jdbc/execute! c [(str "SELECT isomer.create_projection(name => 'jp_counter', kind => 'document', key => 'k', "
                             "database => 'jp', lag => '2 seconds', "
                             "query => 'SELECT c.k, jsonb_build_object(''n'', c.n) AS doc FROM jp_ryw.counters c', "
                             "sources => '{\"jp_ryw.counters\": \"k\"}')")]))))
  (invoke! [_ test op]
    (case (:f op)
      :incr-read
      (let [k (:value op)
            written (try
                      (let [n     (:n (jdbc/execute-one! conn ["UPDATE jp_ryw.counters SET n = n + 1 WHERE k = ? RETURNING n" k]
                                                         {:builder-fn rs/as-unqualified-maps}))
                            token (:t (jdbc/execute-one! conn ["SELECT isomer.last_write_token() AS t"]
                                                         {:builder-fn rs/as-unqualified-maps}))]
                        {:n n :token token})
                      (catch Exception e e))]
        (if (instance? Exception written)
          (outcome op written true)
          (try
            (jdbc/execute! conn [(str "SET isomer.read_after_timeout = '" (:read-after-timeout test) "'")])
            (jdbc/execute! conn [(str "SET isomer.read_after = '" (:token written) "'")])
            (let [doc (:n (jdbc/execute-one! conn ["SELECT (isomer.get('jp_counter', ?::text) ->> 'n')::bigint AS n" (str k)]
                                             {:builder-fn rs/as-unqualified-maps}))]
              (jdbc/execute! conn ["RESET isomer.read_after"])
              (assoc op :type :ok :value {:k k :wrote (:n written) :read doc}))
            (catch Exception e
              ;; The increment committed; only the read did not answer.
              (try (jdbc/execute! conn ["RESET isomer.read_after"]) (catch Exception _))
              (assoc op :type :ok :value {:k k :wrote (:n written)
                                          :read (if (= "IS001" (sql-state e)) :not-yet [:error (sql-state e)])})))))
      :final
      (try
        (let [one (fn [q] (:v (jdbc/execute-one! conn [q] {:builder-fn rs/as-unqualified-maps})))]
          (jdbc/execute! conn ["SELECT isomer.wait_for(isomer.current_token(), 'jp_counter', '60 seconds')"])
          (assoc op :type :ok
                 :value {:total-n (one "SELECT sum(n) AS v FROM jp_ryw.counters")
                         :drift   (one "SELECT count(*) AS v FROM isomer.verify('jp_counter')")}))
        (catch Exception e (assoc op :type :fail :error (.getMessage e))))))
  (teardown! [_ _test])
  (close! [_ _test] (some-> conn .close)))

(defn ryw-checker []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [incr     (filter #(= :incr-read (:f %)) history)
            done     (filter #(= :ok (:type %)) incr)
            maybe    (count (filter #(= :info (:type %)) incr))
            answered (filter #(number? (:read (:value %))) done)
            stale    (filter #(< (:read (:value %)) (:wrote (:value %))) answered)
            final    (:value (last (filter #(and (= :final (:f %)) (= :ok (:type %))) history)))
            total    (:total-n final)
            acked    (count done)
            lost?    (and total (< total acked))
            phantom? (and total (> total (+ acked maybe)))]
        {:valid?           (and (empty? stale) (some? final) (not lost?) (not phantom?) (zero? (or (:drift final) 1)))
         :increments       acked
         :indeterminate    maybe
         :reads-answered   (count answered)
         :reads-not-yet    (count (filter #(= :not-yet (:read (:value %))) done))
         :stale-reads      (count stale)
         :first-stale      (take 5 stale)
         :final            final
         :lost-increments  (when lost? (- acked total))
         :phantom          (when phantom? (- total acked maybe))}))))

;; ── the nemesis: faults injected with the Docker CLI ────────────────────────

(defn sh! [& args]
  (let [p   (.start (ProcessBuilder. ^java.util.List (vec args)))
        out (slurp (.getInputStream p))
        err (slurp (.getErrorStream p))
        rc  (.waitFor p)]
    (when-not (zero? rc) (warn "command failed:" (str/join " " args) err))
    {:rc rc :out out :err err}))

(defn container [test node] (str (:project test) "-" node "-1"))

(defn heal! [test]
  (doseq [n (:nodes test)]
    (let [c (container test n)]
      (sh! "docker" "unpause" c)
      (sh! "docker" "start" c)
      (sh! "docker" "network" "connect" "--alias" n (:network test) c))))

(defn docker-nemesis []
  (reify nemesis/Nemesis
    (setup! [this test] (heal! test) this)
    (invoke! [_ test op]
      (let [node (rand-nth (:nodes test))
            c    (container test node)]
        (case (:f op)
          :pause     (do (sh! "docker" "pause" c) (assoc op :value [:paused node]))
          :kill      (do (sh! "docker" "kill" c) (assoc op :value [:killed node]))
          :partition (do (sh! "docker" "network" "disconnect" (:network test) c) (assoc op :value [:isolated node]))
          :heal      (do (heal! test) (assoc op :value :healed)))))
    (teardown! [_ test] (heal! test))
    nemesis/Reflection
    (fs [_] #{:pause :kill :partition :heal})))

(defn nemesis-gen [faults interval]
  (gen/cycle
    [(gen/sleep interval)
     (gen/once (fn [] {:type :info :f (rand-nth faults)}))   ; a fresh choice every cycle
     (gen/sleep interval)
     {:type :info :f :heal}]))

;; ── tests ───────────────────────────────────────────────────────────────────

(defn isomer-test [opts]
  (let [workload (:workload opts)
        faults   (:faults opts)
        base     (merge tests/noop-test opts
                        {:name      (str "isomerdb-" (name workload) "-" (str/join "+" (map name faults)))
                         :os        os/noop
                         :db        db/noop
                         :ssh       {:dummy? true}
                         :nemesis   (docker-nemesis)
                         :plot      {}})
        nem      (if (seq faults) (nemesis-gen (vec faults) (:fault-interval opts)) nil)
        timed    (fn [client-gen]
                   (gen/phases
                     (gen/time-limit (:time-limit opts)
                                     (if nem (gen/nemesis nem client-gen) (gen/clients client-gen)))
                     (gen/nemesis {:type :info :f :heal})
                     (gen/sleep 15)))]
    (case workload
      :append
      (let [w (append/test {:key-count 12 :min-txn-length 2 :max-txn-length 4 :max-writes-per-key 64
                            :consistency-models [:read-committed]})]
        (assoc base
               :client    (map->AppendClient {})
               :generator (timed (gen/stagger (/ 1 (:rate opts)) (:generator w)))
               :checker   (checker/compose {:append   (:checker w)
                                            :timeline (timeline/html)
                                            :stats    (checker/stats)
                                            :perf     (checker/perf)})))
      :ryw
      (assoc base
             :client    (map->RywClient {})
             :generator (gen/phases
                          (timed (gen/stagger (/ 1 (:rate opts))
                                              (fn [] {:f :incr-read :value (rand-int ryw-keys)})))
                          (gen/clients (gen/once {:f :final})))
             :checker   (checker/compose {:ryw      (ryw-checker)
                                          :timeline (timeline/html)
                                          :stats    (checker/stats)})))))

(def opt-spec
  [[nil "--workload NAME" "append or ryw" :default :append :parse-fn keyword :validate [#{:append :ryw} "append or ryw"]]
   [nil "--faults LIST" "comma-separated: pause, kill, partition (empty: none)" :default [:pause :kill :partition]
    :parse-fn (fn [s] (if (str/blank? s) [] (mapv keyword (str/split s #","))))]
   [nil "--fault-interval SECONDS" "seconds between faults and heals" :default 10 :parse-fn parse-long]
   [nil "--rate HZ" "operations per second, across all clients" :default 50 :parse-fn parse-long]
   [nil "--host HOST" "the coordinator" :default "db"]
   [nil "--pg-password PASSWORD" "postgres's password" :default "isomer"]
   [nil "--project NAME" "the compose project (container names)" :default "isomerdb"]
   [nil "--network NAME" "the compose network" :default "isomerdb_default"]
   [nil "--read-after-timeout DURATION" "isomer.read_after_timeout for ryw reads" :default "10s"]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn isomer-test :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
