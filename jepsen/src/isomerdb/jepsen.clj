(ns isomerdb.jepsen
  "Jepsen tests for IsomerDB, run against the docker-compose cluster (a coordinator and two Citus
  workers) while a nemesis pauses, kills and partitions nodes (the workers, the coordinator, or
  both) and skews their clocks.

  Workloads:

    append  Elle list-append over a table distributed across the nodes: every transaction appends to
            and reads several keys, so most are distributed (two-phase commit). Checked for the
            anomalies READ COMMITTED forbids (G0, G1a, G1b, G1c), and for lost or phantom appends.

    ryw     Read your writes through a projection: a process increments a counter, takes the write's
            token, and reads the counter's document projection with SET isomer.read_after = token.
            A read that answers must show the increment or a later one; the alternative it is allowed
            is an explicit \"not yet\" (IS001). At the end, every acknowledged increment must be in
            the table, and the projection must equal recomputation.

    mongo   The same through the MongoDB wire protocol: the increment is written over SQL, its token
            becomes the operationTime of a causally consistent driver session, and the document is
            read through the gateway, which must wait for it or answer \"not yet\" (ExceededTimeLimit).

    incremental
            An incrementally maintained projection (sales per category, from order lines sharded by
            order through products and a reference table) under random inserts, updates, deletes,
            product moves and statements spanning shards. Its state is folded from row deltas pulled
            from the workers exactly once; at the end it must equal recomputation, with nothing left
            queued on the workers and no pull left prepared."
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
  (:import (java.sql SQLException)
           (java.security.cert X509Certificate)
           (javax.net.ssl SSLContext TrustManager X509TrustManager)
           (com.mongodb Block ClientSessionOptions ConnectionString MongoClientSettings MongoException)
           (com.mongodb.client MongoClient MongoClients)
           (com.mongodb.client.model Filters)
           (org.bson BsonTimestamp)))

;; ── connections ─────────────────────────────────────────────────────────────

(defn datasource [test]
  (jdbc/get-datasource
    {:jdbcUrl (str "jdbc:postgresql://" (:host test) ":5432/postgres"
                   "?user=postgres&password=" (:pg-password test)
                   "&connectTimeout=5&socketTimeout=15&tcpKeepAlive=true")}))

(defn setup-datasource
  "For creating schemas and projections once per run: those statements can outlast the clients'
  socket timeout, on a slow node (the clock-skew image intercepts every clock read)."
  [test]
  (jdbc/get-datasource
    {:jdbcUrl (str "jdbc:postgresql://" (:host test) ":5432/postgres"
                   "?user=postgres&password=" (:pg-password test)
                   "&connectTimeout=10&socketTimeout=600&tcpKeepAlive=true")}))

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

;; A fault on the coordinator closes the clients' connections; a client whose last operation
;; failed (rather than ending indeterminate) is not reopened by Jepsen, so it reconnects itself.
(defn live!
  "The client's connection, reconnected if a fault closed it."
  ^java.sql.Connection [{:keys [ds conn]}]
  (let [c @conn]
    (if (and c (not (.isClosed ^java.sql.Connection c)))
      c
      (do (try (some-> c .close) (catch Exception _))
          (reset! conn (jdbc/get-connection ds))))))

(defn with-retries
  "Run (f connection), retrying for up to a minute while the cluster recovers from the last fault:
  the final checks must not fail just because the coordinator is still restarting."
  [client f]
  (loop [tries 30]
    (let [r (try {:ok (f (live! client))}
                 (catch Exception e
                   (if (pos? tries) ::retry (throw e))))]
      (if (= r ::retry)
        (do (try (some-> @(:conn client) .close) (catch Exception _))
            (Thread/sleep 2000)
            (recur (dec tries)))
        (:ok r)))))

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
      (assoc this :ds ds :conn (atom (jdbc/get-connection ds)))))
  (setup! [_ test]
    (once! :append #(with-open [c (jdbc/get-connection (setup-datasource test))]
      (jdbc/execute! c ["DROP SCHEMA IF EXISTS jp_append CASCADE"])
      (jdbc/execute! c ["CREATE SCHEMA jp_append"])
      (jdbc/execute! c ["CREATE TABLE jp_append.lists (k bigint PRIMARY KEY, v text NOT NULL)"])
      (jdbc/execute! c ["SELECT isomer.distribute(ARRAY['jp_append.lists']::regclass[], 'k', 8)"]))))
  (invoke! [this _test op]
    (let [conn   (live! this)
          txn    (:value op)
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
  (close! [_ _test] (some-> conn deref .close)))

;; ── the read-your-writes workload ───────────────────────────────────────────

(def ryw-keys 20)

(defrecord RywClient [ds conn]
  client/Client
  (open! [this test _node]
    (let [ds (datasource test)]
      (assoc this :ds ds :conn (atom (jdbc/get-connection ds)))))
  (setup! [_ test]
    (once! :ryw #(with-open [c (jdbc/get-connection (setup-datasource test))]
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
  (invoke! [this test op]
    (case (:f op)
      :incr-read
      (let [conn (live! this)
            k    (:value op)
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
        (with-retries this
          (fn [conn]
            (let [one (fn [q] (:v (jdbc/execute-one! conn [q] {:builder-fn rs/as-unqualified-maps})))]
              (jdbc/execute! conn ["SELECT isomer.wait_for(isomer.current_token(), 'jp_counter', '60 seconds')"])
              (assoc op :type :ok
                     :value {:total-n (one "SELECT sum(n) AS v FROM jp_ryw.counters")
                             :drift   (one "SELECT count(*) AS v FROM isomer.verify('jp_counter')")}))))
        (catch Exception e (assoc op :type :fail :error (.getMessage e))))))
  (teardown! [_ _test])
  (close! [_ _test] (some-> conn deref .close)))

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

;; ── the MongoDB read-your-writes workload ───────────────────────────────────

(defn trust-all
  "The gateway's development certificate is self-signed."
  ^SSLContext []
  (doto (SSLContext/getInstance "TLS")
    (.init nil
           (into-array TrustManager
                       [(reify X509TrustManager
                          (checkClientTrusted [_ _ _])
                          (checkServerTrusted [_ _ _])
                          (getAcceptedIssuers [_] (make-array X509Certificate 0)))])
           nil)))

(defn mongo-client ^MongoClient [test]
  (MongoClients/create
    (-> (MongoClientSettings/builder)
        (.applyConnectionString (ConnectionString. (str "mongodb://isomer:isomer@" (:gateway test)
                                                        "/?tls=true&connectTimeoutMS=5000&socketTimeoutMS=15000"
                                                        "&serverSelectionTimeoutMS=5000")))
        (.applyToSslSettings (reify Block
                               (apply [_ b] (-> ^com.mongodb.connection.SslSettings$Builder b
                                                (.enabled true) (.invalidHostNameAllowed true) (.context (trust-all))))))
        (.build))))

(defrecord MongoRywClient [ds conn mongo]
  client/Client
  (open! [this test _node]
    (let [ds (datasource test)]
      (assoc this :ds ds :conn (atom (jdbc/get-connection ds)) :mongo (mongo-client test))))
  (setup! [_ test]
    (once! :mongo #(with-open [c (jdbc/get-connection (setup-datasource test))]
      (jdbc/execute! c ["SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name = 'jp_mcounter'"])
      (jdbc/execute! c ["DROP SCHEMA IF EXISTS jp_mongo CASCADE"])
      (jdbc/execute! c ["CREATE SCHEMA jp_mongo"])
      (jdbc/execute! c ["CREATE TABLE jp_mongo.counters (k bigint PRIMARY KEY, n bigint NOT NULL)"])
      (jdbc/execute! c ["SELECT isomer.distribute(ARRAY['jp_mongo.counters']::regclass[], 'k', 8)"])
      (jdbc/execute! c [(str "INSERT INTO jp_mongo.counters SELECT g, 0 FROM generate_series(0, " (dec ryw-keys) ") g")])
      (jdbc/execute! c [(str "SELECT isomer.create_projection(name => 'jp_mcounter', kind => 'document', key => 'k', "
                             "database => 'jp', lag => '2 seconds', "
                             "query => 'SELECT c.k, jsonb_build_object(''n'', c.n) AS doc FROM jp_mongo.counters c', "
                             "sources => '{\"jp_mongo.counters\": \"k\"}')")]))))
  (invoke! [this _test op]
    (case (:f op)
      :incr-read
      (let [conn    (live! this)
            k       (:value op)
            written (try
                      (let [n  (:n (jdbc/execute-one! conn ["UPDATE jp_mongo.counters SET n = n + 1 WHERE k = ? RETURNING n" k]
                                                      {:builder-fn rs/as-unqualified-maps}))
                            ts (jdbc/execute-one! conn ["SELECT t, i FROM isomer.token_to_mongo_timestamp(isomer.last_write_token())"]
                                                  {:builder-fn rs/as-unqualified-maps})]
                        {:n n :t (:t ts) :i (:i ts)})
                      (catch Exception e e))]
        (if (instance? Exception written)
          (outcome op written true)
          (try
            (with-open [s (.startSession ^MongoClient mongo (-> (ClientSessionOptions/builder) (.causallyConsistent true) (.build)))]
              ;; Both halves are unsigned 32-bit; the increment's top bit is often set.
              (.advanceOperationTime s (BsonTimestamp. (unchecked-int (:t written)) (unchecked-int (:i written))))
              (let [doc (-> (.getDatabase ^MongoClient mongo "jp") (.getCollection "jp_mcounter")
                            (.find s (Filters/eq "_id" (long k))) (.first))]
                (assoc op :type :ok :value {:k k :wrote (:n written) :read (some-> doc (.get "n") long)})))
            (catch MongoException e
              ;; The increment committed; only the read did not answer. "Not yet" is
              ;; ExceededTimeLimit (50), which the driver raises as MongoExecutionTimeoutException.
              (assoc op :type :ok :value {:k k :wrote (:n written)
                                          :read (if (= 50 (.getCode e)) :not-yet [:error (.getMessage e)])}))
            (catch Exception e
              (assoc op :type :ok :value {:k k :wrote (:n written) :read [:error (.getMessage e)]})))))
      :final
      (try
        (with-retries this
          (fn [conn]
            (let [one (fn [q] (:v (jdbc/execute-one! conn [q] {:builder-fn rs/as-unqualified-maps})))]
              (jdbc/execute! conn ["SELECT isomer.wait_for(isomer.current_token(), 'jp_mcounter', '60 seconds')"])
              (assoc op :type :ok
                     :value {:total-n (one "SELECT sum(n) AS v FROM jp_mongo.counters")
                             :drift   (one "SELECT count(*) AS v FROM isomer.verify('jp_mcounter')")}))))
        (catch Exception e (assoc op :type :fail :error (.getMessage e))))))
  (teardown! [_ _test])
  (close! [_ _test]
    (some-> conn deref .close)
    (some-> ^MongoClient mongo .close)))

;; ── the incremental workload ────────────────────────────────────────────────

(def inc-products 60)
(def inc-categories 6)
(def inc-orders 2000)

(def inc-query
  (str "SELECT c.id, jsonb_build_object(''name'', c.name, ''units'', sum(l.qty), ''revenue'', sum(l.qty * l.price), "
       "''lines'', count(*), ''top'', max(l.price)) AS doc "
       "FROM jp_inc.lines l JOIN jp_inc.products p ON p.id = l.product_id JOIN jp_inc.categories c ON c.id = p.category_id "
       "WHERE l.status = ''paid'' GROUP BY c.id"))

(defn inc-op []
  (let [o (rand-int inc-orders)]
    (rand-nth [[:insert o (rand-int 4) (inc (rand-int inc-products)) (inc (rand-int 5))]
               [:insert o (rand-int 4) (inc (rand-int inc-products)) (inc (rand-int 5))]
               [:update o]
               [:void o]
               [:delete o (rand-int 4)]
               [:move (inc (rand-int inc-products)) (inc (rand-int inc-categories))]
               [:product-wide (inc (rand-int inc-products))]])))

(defrecord IncClient [ds conn]
  client/Client
  (open! [this test _node]
    (let [ds (datasource test)]
      (assoc this :ds ds :conn (atom (jdbc/get-connection ds)))))
  (setup! [_ test]
    (once! :incremental #(with-open [c (jdbc/get-connection (setup-datasource test))]
      (jdbc/execute! c ["SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name = 'jp_cat_sales'"])
      (jdbc/execute! c ["DROP SCHEMA IF EXISTS jp_inc CASCADE"])
      (jdbc/execute! c ["CREATE SCHEMA jp_inc"])
      (jdbc/execute! c ["CREATE TABLE jp_inc.categories (id int PRIMARY KEY, name text NOT NULL)"])
      (jdbc/execute! c ["CREATE TABLE jp_inc.products (id int PRIMARY KEY, category_id int NOT NULL)"])
      (jdbc/execute! c [(str "CREATE TABLE jp_inc.lines (order_id bigint NOT NULL, line int NOT NULL, product_id int NOT NULL, "
                             "qty int NOT NULL, price numeric NOT NULL, status text NOT NULL, PRIMARY KEY (order_id, line))")])
      (jdbc/execute! c ["SELECT create_reference_table('jp_inc.categories')"])
      (jdbc/execute! c ["SELECT create_distributed_table('jp_inc.products', 'id', shard_count => 8)"])
      (jdbc/execute! c ["SELECT create_distributed_table('jp_inc.lines', 'order_id', shard_count => 8, colocate_with => 'none')"])
      (jdbc/execute! c [(str "INSERT INTO jp_inc.categories SELECT g, 'category ' || g FROM generate_series(1, " inc-categories ") g")])
      (jdbc/execute! c [(str "INSERT INTO jp_inc.products SELECT g, 1 + g % " inc-categories " FROM generate_series(1, " inc-products ") g")])
      (jdbc/execute! c [(str "INSERT INTO jp_inc.lines SELECT g / 2, g % 2, 1 + g % " inc-products ", 1 + g % 5, (g % 13) + 0.5, 'paid' "
                             "FROM generate_series(0, " (* 2 inc-orders) ") g")])
      (jdbc/execute! c [(str "SELECT isomer.create_projection(name => 'jp_cat_sales', kind => 'document', key => 'id', "
                             "database => 'jp', lag => '2 seconds', strategy => 'incremental', query => '" inc-query "')")]))))
  (invoke! [this _test op]
    (case (:f op)
      :write
      (let [conn           (live! this)
            [kind a b c d] (:value op)]
        (try
          (case kind
            :insert       (jdbc/execute! conn [(str "INSERT INTO jp_inc.lines VALUES (?, ?, ?, ?, 2.5, 'paid') "
                                                    "ON CONFLICT (order_id, line) DO UPDATE SET qty = EXCLUDED.qty, product_id = EXCLUDED.product_id")
                                               a b c d])
            :update       (jdbc/execute! conn ["UPDATE jp_inc.lines SET qty = qty + 1 WHERE order_id = ?" a])
            :void         (jdbc/execute! conn ["UPDATE jp_inc.lines SET status = CASE WHEN status = 'paid' THEN 'void' ELSE 'paid' END WHERE order_id = ?" a])
            :delete       (jdbc/execute! conn ["DELETE FROM jp_inc.lines WHERE order_id = ? AND line = ?" a b])
            :move         (jdbc/execute! conn ["UPDATE jp_inc.products SET category_id = ? WHERE id = ?" b a])
            ;; One statement across every shard of the lines: a distributed transaction whose deltas
            ;; are captured on several nodes.
            :product-wide (jdbc/execute! conn ["UPDATE jp_inc.lines SET price = price + 1 WHERE product_id = ?" a]))
          (assoc op :type :ok)
          (catch Exception e (outcome op e true))))
      :final
      (try
        (with-retries this
          (fn [conn]
            (let [one (fn [q] (:v (jdbc/execute-one! conn [q] {:builder-fn rs/as-unqualified-maps})))]
              (jdbc/execute! conn ["SELECT isomer.wait_for(isomer.current_token(), 'jp_cat_sales', '120 seconds')"])
              (assoc op :type :ok
                     :value {:drift          (one "SELECT count(*) AS v FROM isomer.verify('jp_cat_sales')")
                             :worker-deltas  (one "SELECT coalesce(sum(result::bigint), 0) AS v FROM run_command_on_workers('SELECT count(*) FROM isomer.node_deltas')")
                             :prepared-pulls (one (str "SELECT coalesce(sum(result::bigint), 0) AS v FROM run_command_on_workers("
                                                       "$$SELECT count(*) FROM pg_prepared_xacts WHERE gid LIKE 'isomer\\_pull\\_%'$$)"))
                             :deltas-folded  (one "SELECT deltas_folded AS v FROM isomer.projection_stats WHERE projection = 'jp_cat_sales'")
                             :certificate    (one "SELECT isomer.certificate('jp_cat_sales') ->> 'valid' AS v")}))))
        (catch Exception e (assoc op :type :fail :error (.getMessage e))))))
  (teardown! [_ _test])
  (close! [_ _test] (some-> conn deref .close)))

(defn inc-checker []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [writes (filter #(= :write (:f %)) history)
            final  (:value (last (filter #(and (= :final (:f %)) (= :ok (:type %))) history)))]
        {:valid?        (and (some? final) (zero? (:drift final)) (zero? (:worker-deltas final))
                             (zero? (:prepared-pulls final)) (= "true" (:certificate final)))
         :writes        (count (filter #(= :ok (:type %)) writes))
         :indeterminate (count (filter #(= :info (:type %)) writes))
         :failed        (count (filter #(= :fail (:type %)) writes))
         :final         final}))))

;; ── the nemesis: faults injected with the Docker CLI ────────────────────────

(defn sh! [& args]
  (let [p   (.start (ProcessBuilder. ^java.util.List (vec args)))
        out (slurp (.getInputStream p))
        err (slurp (.getErrorStream p))
        rc  (.waitFor p)]
    (when-not (zero? rc) (warn "command failed:" (str/join " " args) err))
    {:rc rc :out out :err err}))

(defn container [test node] (str (:project test) "-" node "-1"))

;; Clock skew needs nodes started with libfaketime (tests/jepsen.sh with SKEW=1): each reads its
;; offset in seconds, e.g. "+45" or "-30", from this file on every call.
(def faketime-file "/tmp/isomer-faketime")

(defn skew! [test node seconds]
  (sh! "docker" "exec" (container test node) "sh" "-c"
       (str "echo " (if (neg? seconds) seconds (str "+" seconds)) " > " faketime-file)))

(defn heal! [test]
  (doseq [n (:fault-nodes test)]
    (let [c (container test n)]
      (sh! "docker" "unpause" c)
      (sh! "docker" "start" c)
      (sh! "docker" "network" "connect" "--alias" n (:network test) c)
      (when (:skew test) (skew! test n 0)))))

(defn docker-nemesis []
  (reify nemesis/Nemesis
    (setup! [this test] (heal! test) this)
    (invoke! [_ test op]
      (let [node (rand-nth (:fault-nodes test))
            c    (container test node)]
        (case (:f op)
          :pause     (do (sh! "docker" "pause" c) (assoc op :value [:paused node]))
          :kill      (do (sh! "docker" "kill" c) (assoc op :value [:killed node]))
          :partition (do (sh! "docker" "network" "disconnect" (:network test) c) (assoc op :value [:isolated node]))
          :skew      (let [s (* (rand-nth [-1 1]) (+ 5 (rand-int 56)))]
                       (skew! test node s)
                       (assoc op :value [:skewed node s]))
          :heal      (do (heal! test) (assoc op :value :healed)))))
    (teardown! [_ test] (heal! test))
    nemesis/Reflection
    (fs [_] #{:pause :kill :partition :skew :heal})))

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
                        {:name      (str "isomerdb-" (name workload) "-" (str/join "+" (map name faults))
                                         "-on-" (str/join "+" (:fault-nodes opts)))
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
      ;; READ COMMITTED forbids G0, G1a, G1b and G1c; Elle looks for exactly those (and internal
      ;; inconsistency), so a search for anomalies RC allows can't time out into "unknown".
      (let [w (append/test {:key-count 12 :min-txn-length 2 :max-txn-length 4 :max-writes-per-key 64
                            :consistency-models [:read-committed]
                            :anomalies [:G0 :G1a :G1b :G1c :internal]
                            :cycle-search-timeout 30000})]
        (assoc base
               :client    (map->AppendClient {})
               :generator (timed (gen/stagger (/ 1 (:rate opts)) (:generator w)))
               :checker   (checker/compose {:append   (:checker w)
                                            :timeline (timeline/html)
                                            :stats    (checker/stats)
                                            :perf     (checker/perf)})))
      (:ryw :mongo)
      (assoc base
             :client    (if (= workload :ryw) (map->RywClient {}) (map->MongoRywClient {}))
             :generator (gen/phases
                          (timed (gen/stagger (/ 1 (:rate opts))
                                              (fn [] {:f :incr-read :value (rand-int ryw-keys)})))
                          (gen/clients (gen/once {:f :final})))
             :checker   (checker/compose {:ryw      (ryw-checker)
                                          :timeline (timeline/html)
                                          :stats    (checker/stats)}))
      :incremental
      (assoc base
             :client    (map->IncClient {})
             :generator (gen/phases
                          (timed (gen/stagger (/ 1 (:rate opts)) (fn [] {:f :write :value (inc-op)})))
                          (gen/clients (gen/once {:f :final})))
             :checker   (checker/compose {:incremental (inc-checker)
                                          :timeline    (timeline/html)
                                          :stats       (checker/stats)})))))

(def opt-spec
  [[nil "--workload NAME" "append, ryw, mongo or incremental" :default :append :parse-fn keyword
    :validate [#{:append :ryw :mongo :incremental} "append, ryw, mongo or incremental"]]
   [nil "--fault-nodes LIST" "the containers faults hit: worker1, worker2, db (the coordinator)" :default ["worker1" "worker2"]
    :parse-fn (fn [s] (str/split s #","))]
   [nil "--skew" "the nodes run with libfaketime, so their clocks can be skewed (add skew to --faults)" :default false]
   [nil "--gateway HOST:PORT" "the MongoDB gateway" :default "gateway:10260"]
   [nil "--faults LIST" "comma-separated: pause, kill, partition, skew (empty: none)" :default [:pause :kill :partition]
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
