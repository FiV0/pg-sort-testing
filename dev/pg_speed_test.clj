(ns pg-speed-test
  (:require [next.jdbc :as jdbc]))

;; assumes you have created a db named "example"
(def conn (jdbc/get-connection (jdbc/get-datasource {:dbtype "postgres"
                                                     :dbname "example"
                                                     :user "postgres"
                                                     :password "postgres"
                                                     :reWriteBatchedInserts true})))

(.close conn)

(jdbc/execute! conn ["CREATE TABLE docs (
id integer PRIMARY KEY,
int integer,
long bigint,
str varchar(32));"])

(jdbc/execute! conn ["DROP TABLE docs;"])

(defn rand-str [l]
  (apply str (repeatedly l #(rand-nth "abcdefghijklmnopqrstuvwxyz0123456789"))))

(defn rand-long
  "Returns a random integer between 0 (inclusive) and n (exclusive)."
  [n] (long (rand n)))

(defn sort-doc [i] [i (rand-int Integer/MAX_VALUE) (rand-long Long/MAX_VALUE) (rand-str 32)])

(with-open [ps (jdbc/prepare conn ["INSERT INTO docs (id,int,long,str) values (?,?,?,?)"])]

  (doseq [tx (->> (range 1000000)
                  (map sort-doc)
                  (partition-all 1024))]
    (jdbc/execute-batch! ps (vec tx))))

(time (jdbc/execute! conn ["SELECT COUNT(*) FROM docs"]))

(time (jdbc/execute! conn ["SELECT docs.int, docs.long, docs.str FROM docs ORDER BY docs.int LIMIT 10"]))
(time (jdbc/execute! conn ["SELECT docs.int, docs.long, docs.str FROM docs ORDER BY docs.long LIMIT 10"]))
(time (jdbc/execute! conn ["SELECT docs.int, docs.long, docs.str FROM docs ORDER BY docs.str LIMIT 10"]))

(time (jdbc/execute! conn ["EXPLAIN ANALYZE SELECT docs.int, docs.long, docs.str FROM docs ORDER BY docs.int LIMIT 10"]))
(time (jdbc/execute! conn ["EXPLAIN ANALYZE SELECT docs.int, docs.long, docs.str FROM docs ORDER BY docs.int LIMIT 14000"]))
