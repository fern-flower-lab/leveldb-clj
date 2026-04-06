(ns leveldb-clj.tx-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [leveldb-clj.core :as leveldb]
            [leveldb-clj.kv :as kv]
            [leveldb-clj.tx :as tx]
            [clojure.java.io :as io])
  (:import [leveldb_clj.tx Binary]))

(def ^:dynamic *store* nil)
(def ^:dynamic *db-path* nil)

(defn delete-recursively [f]
  (when (.exists f)
    (when (.isDirectory f)
      (doseq [child (.listFiles f)]
        (delete-recursively child)))
    (.delete f)))

(defn with-temp-db [f]
  (let [path (str "target/test-tx-db-" (System/currentTimeMillis))]
    (try
      (binding [*db-path* path
                *store* (leveldb/map->LevelDBStore {:path path})]
        (f))
      (finally
        (when *store*
          (.close *store*))
        (delete-recursively (io/file path))))))

(use-fixtures :each with-temp-db)

;; Binary type tests

(deftest binary-equality-test
  (testing "Binary wraps byte arrays with proper equality"
    (let [b1 (tx/->Binary (.getBytes "test"))
          b2 (tx/->Binary (.getBytes "test"))
          b3 (tx/->Binary (.getBytes "other"))]
      (is (.equals b1 b2))
      (is (not (.equals b1 b3))))))

(deftest binary-equality-with-foreign-type-test
  (testing "Binary equality returns false for non-Binary values"
    (let [b (tx/->Binary (.getBytes "test"))]
      (is (false? (.equals b "test")))
      (is (false? (.equals b nil))))))

(deftest binary-hashcode-test
  (testing "Binary has content-based hashCode"
    (let [b1 (tx/->Binary (.getBytes "test"))
          b2 (tx/->Binary (.getBytes "test"))
          b3 (tx/->Binary (.getBytes "other"))]
      (is (= (.hashCode b1) (.hashCode b2)))
      ;; Different content should (usually) have different hash
      (is (not= (.hashCode b1) (.hashCode b3))))))

(deftest binary-as-map-key-test
  (testing "Binary works correctly as map key"
    (let [b1 (tx/->Binary (.getBytes "key"))
          b2 (tx/->Binary (.getBytes "key"))
          m {b1 "value"}]
      ;; Should find value with equal Binary key
      (is (= "value" (get m b2))))))

;; Transaction tests

(deftest transaction-add-commit-test
  (testing "add and commit persists to store"
    (let [atomic (tx/->LevelDB-TX *store* (atom {}))]
      (tx/add atomic (.getBytes "k1") (.getBytes "v1"))
      ;; Not visible before commit
      (is (nil? (kv/retrieve *store* (.getBytes "k1"))))
      (tx/commit atomic)
      ;; Visible after commit
      (is (= "v1" (String. (kv/retrieve *store* (.getBytes "k1"))))))))

(deftest transaction-add-batch-test
  (testing "add-batch adds multiple entries"
    (let [atomic (tx/->LevelDB-TX *store* (atom {}))]
      (tx/add-batch atomic {(.getBytes "a") (.getBytes "1")
                           (.getBytes "b") (.getBytes "2")})
      (tx/commit atomic)
      (is (= "1" (String. (kv/retrieve *store* (.getBytes "a")))))
      (is (= "2" (String. (kv/retrieve *store* (.getBytes "b"))))))))

(deftest transaction-rollback-test
  (testing "rollback discards pending changes"
    (let [atomic (tx/->LevelDB-TX *store* (atom {}))]
      (tx/add atomic (.getBytes "k1") (.getBytes "v1"))
      (tx/rollback atomic)
      (tx/commit atomic)
      ;; Should not be persisted
      (is (nil? (kv/retrieve *store* (.getBytes "k1")))))))

(deftest transaction-del-test
  (testing "del removes from pending state"
    (let [atomic (tx/->LevelDB-TX *store* (atom {}))]
      (tx/add atomic (.getBytes "k1") (.getBytes "v1"))
      (tx/add atomic (.getBytes "k2") (.getBytes "v2"))
      (tx/del atomic (.getBytes "k1"))
      (tx/commit atomic)
      ;; k1 was removed before commit
      (is (nil? (kv/retrieve *store* (.getBytes "k1"))))
      ;; k2 was committed
      (is (= "v2" (String. (kv/retrieve *store* (.getBytes "k2"))))))))

(deftest transaction-del-persisted-key-test
  (testing "del removes an already-persisted key on commit"
    (kv/insert *store* (.getBytes "k1") (.getBytes "v1"))
    (let [atomic (tx/->LevelDB-TX *store* (atom {}))]
      (tx/del atomic (.getBytes "k1"))
      (tx/commit atomic)
      (is (nil? (kv/retrieve *store* (.getBytes "k1")))))))

(deftest transaction-isolation-test
  (testing "uncommitted changes are isolated from store reads"
    (let [atomic (tx/->LevelDB-TX *store* (atom {}))]
      ;; Pre-existing data
      (kv/insert *store* (.getBytes "existing") (.getBytes "old"))
      ;; Add to transaction (not committed)
      (tx/add atomic (.getBytes "existing") (.getBytes "new"))
      ;; Store still sees old value
      (is (= "old" (String. (kv/retrieve *store* (.getBytes "existing")))))
      ;; After commit, store sees new value
      (tx/commit atomic)
      (is (= "new" (String. (kv/retrieve *store* (.getBytes "existing"))))))))

(deftest transaction-fluent-api-test
  (testing "transaction operations return this for chaining"
    (let [atomic (tx/->LevelDB-TX *store* (atom {}))]
      (-> atomic
          (tx/add (.getBytes "a") (.getBytes "1"))
          (tx/add (.getBytes "b") (.getBytes "2"))
          (tx/del (.getBytes "a"))
          (tx/commit))
      (is (nil? (kv/retrieve *store* (.getBytes "a"))))
      (is (= "2" (String. (kv/retrieve *store* (.getBytes "b"))))))))

(deftest transaction-multiple-commits-test
  (testing "transaction can be reused for multiple commits"
    (let [atomic (tx/->LevelDB-TX *store* (atom {}))]
      ;; First batch
      (tx/add atomic (.getBytes "k1") (.getBytes "v1"))
      (tx/commit atomic)
      ;; Second batch
      (tx/add atomic (.getBytes "k2") (.getBytes "v2"))
      (tx/commit atomic)
      ;; Both should be persisted
      (is (= "v1" (String. (kv/retrieve *store* (.getBytes "k1")))))
      (is (= "v2" (String. (kv/retrieve *store* (.getBytes "k2"))))))))

(deftest transaction-empty-commit-test
  (testing "committing empty transaction is safe"
    (let [atomic (tx/->LevelDB-TX *store* (atom {}))]
      (tx/commit atomic)
      ;; Should not throw, store should be unchanged
      (is (empty? (kv/list-keys *store*))))))
