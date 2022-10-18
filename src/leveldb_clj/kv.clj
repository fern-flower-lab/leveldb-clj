(ns leveldb-clj.kv
  (:import (clojure.lang IPersistentMap)))

(defprotocol KeyValueStore
  (retrieve [store k]
    "Return the value associated with the given key.")
  (insert [store k v]
    "Return a new store, with an additional `k` -> `v` mapping.")
  (delete [store k]
    "Return a new store, without the given key.")
  (insert-batch [store m]
    "Return a new store, with additional mappings.")
  (list-keys [store]
    "Returns a seq of keys existing in the store")
  (stream [store]
    "Returns sequence on KV pair"))

(extend-type IPersistentMap
  KeyValueStore
  (retrieve [m k]
    (get m k))
  (insert [m k v]
    (assoc m k v))
  (delete [m k]
    (dissoc m k))
  (insert-batch [m other]
    (merge m other))
  (list-keys [m]
    (keys m))
  (stream [m]
    (map (fn [[k v]] [k v]) m)))
