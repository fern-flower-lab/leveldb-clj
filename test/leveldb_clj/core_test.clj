(ns leveldb-clj.core-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [leveldb-clj.core :as leveldb]
            [leveldb-clj.kv :as kv]
            [clojure.java.io :as io]))

(def ^:dynamic *store* nil)
(def ^:dynamic *db-path* nil)

(defn delete-recursively [f]
  (when (.exists f)
    (when (.isDirectory f)
      (doseq [child (.listFiles f)]
        (delete-recursively child)))
    (.delete f)))

(defn with-temp-db [f]
  (let [path (str "target/test-db-" (System/currentTimeMillis))]
    (try
      (binding [*db-path* path
                *store* (leveldb/map->LevelDBStore {:path path})]
        (f))
      (finally
        (when *store*
          (.close *store*))
        (delete-recursively (io/file path))))))

(use-fixtures :each with-temp-db)

(deftest leveldb-store-basic-operations-test
  (testing "insert and retrieve"
    (kv/insert *store* (.getBytes "key1") (.getBytes "value1"))
    (is (= "value1" (String. (kv/retrieve *store* (.getBytes "key1"))))))

  (testing "retrieve missing key returns nil"
    (is (nil? (kv/retrieve *store* (.getBytes "nonexistent")))))

  (testing "delete"
    (kv/insert *store* (.getBytes "to-delete") (.getBytes "value"))
    (is (some? (kv/retrieve *store* (.getBytes "to-delete"))))
    (kv/delete *store* (.getBytes "to-delete"))
    (is (nil? (kv/retrieve *store* (.getBytes "to-delete")))))

  (testing "overwrite existing key"
    (kv/insert *store* (.getBytes "key") (.getBytes "old"))
    (kv/insert *store* (.getBytes "key") (.getBytes "new"))
    (is (= "new" (String. (kv/retrieve *store* (.getBytes "key")))))))

(deftest leveldb-store-batch-operations-test
  (testing "insert-batch"
    (kv/insert-batch *store* {(.getBytes "a") (.getBytes "1")
                              (.getBytes "b") (.getBytes "2")
                              (.getBytes "c") (.getBytes "3")})
    (is (= "1" (String. (kv/retrieve *store* (.getBytes "a")))))
    (is (= "2" (String. (kv/retrieve *store* (.getBytes "b")))))
    (is (= "3" (String. (kv/retrieve *store* (.getBytes "c")))))))

(deftest leveldb-store-list-keys-test
  (testing "list-keys returns all keys"
    (kv/insert-batch *store* {(.getBytes "x") (.getBytes "1")
                              (.getBytes "y") (.getBytes "2")})
    (let [keys (set (map #(String. %) (kv/list-keys *store*)))]
      (is (contains? keys "x"))
      (is (contains? keys "y")))))

(deftest leveldb-store-stream-test
  (testing "stream returns closeable sequence of key/value pairs"
    (kv/insert *store* (.getBytes "k1") (.getBytes "v1"))
    (with-open [s (kv/stream *store*)]
      (let [entries (doall (seq s))]
        (is (= 1 (count entries)))
        (is (= ["k1" "v1"]
               (mapv #(String. %) (first entries))))))))

(deftest leveldb-store-codec-test
  (testing "codec is applied consistently across reads and iteration"
    (let [codec (reify leveldb_clj.core.LevelDBCodec
                  (outgoing-key [_ k] (.getBytes (str "key:" k)))
                  (incoming-key [_ k] (subs (String. k) 4))
                  (outgoing-value [_ v] (.getBytes (str "val:" v)))
                  (incoming-value [_ v] (subs (String. v) 4)))
          path (str "target/test-codec-" (System/currentTimeMillis))]
      (try
        (with-open [store (leveldb/map->LevelDBStore {:path path :codec codec})]
          (kv/insert store "a" "b")
          (is (= "b" (kv/retrieve store "a")))
          (is (= ["a"] (vec (kv/list-keys store))))
          (with-open [s (kv/stream store)]
            (is (= [["a" "b"]] (vec (seq s))))))
        (finally
          (delete-recursively (io/file path)))))))

(deftest leveldb-store-closeable-test
  (testing "store can be closed"
    (let [path (str "target/test-close-" (System/currentTimeMillis))
          store (leveldb/map->LevelDBStore {:path path})]
      (try
        (kv/insert store (.getBytes "test") (.getBytes "value"))
        (.close store)
        ;; Reopening should work after close
        (with-open [store2 (leveldb/map->LevelDBStore {:path path})]
          (is (= "value" (String. (kv/retrieve store2 (.getBytes "test"))))))
        (finally
          (delete-recursively (io/file path)))))))

(deftest leveldb-store-fluent-api-test
  (testing "operations return store for chaining"
    (let [result (-> *store*
                     (kv/insert (.getBytes "a") (.getBytes "1"))
                     (kv/insert (.getBytes "b") (.getBytes "2"))
                     (kv/delete (.getBytes "a")))]
      (is (instance? leveldb_clj.core.LevelDBStore result))
      (is (nil? (kv/retrieve result (.getBytes "a"))))
      (is (= "2" (String. (kv/retrieve result (.getBytes "b"))))))))
