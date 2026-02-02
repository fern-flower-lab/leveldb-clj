(ns leveldb-clj.kv-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [leveldb-clj.kv :as kv]))

(deftest persistent-map-protocol-test
  (testing "IPersistentMap implements KeyValueStore protocol"
    (let [store {}]
      (testing "insert and retrieve"
        (let [store' (kv/insert store :a 1)]
          (is (= 1 (kv/retrieve store' :a)))
          (is (nil? (kv/retrieve store' :b)))))

      (testing "delete"
        (let [store' (-> store
                         (kv/insert :a 1)
                         (kv/delete :a))]
          (is (nil? (kv/retrieve store' :a)))))

      (testing "insert-batch"
        (let [store' (kv/insert-batch store {:a 1 :b 2 :c 3})]
          (is (= 1 (kv/retrieve store' :a)))
          (is (= 2 (kv/retrieve store' :b)))
          (is (= 3 (kv/retrieve store' :c)))))

      (testing "list-keys"
        (let [store' (kv/insert-batch store {:a 1 :b 2})]
          (is (= #{:a :b} (set (kv/list-keys store'))))))

      (testing "stream returns closeable"
        (let [store' (kv/insert-batch store {:a 1 :b 2})]
          (with-open [s (kv/stream store')]
            (is (= #{[:a 1] [:b 2]} (set (seq s))))))))))
