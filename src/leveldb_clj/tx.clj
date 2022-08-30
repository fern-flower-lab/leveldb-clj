(ns leveldb-clj.tx
  (:require [leveldb-clj.kv :as kv]))

(defprotocol Transactional
  (add [_ k v] "Add k->v pair to transaction")
  (add-batch [_ m] "Add multiple k->v pairs to transaction")
  (del [_ k] "Delete k->v pair from transaction")
  (commit [_] "Commit transaction (push prepared data to the store)")
  (rollback [_] "Drop yet not stored data from the internal state (reset)"))

(defrecord LevelDB-TX [store state]
  Transactional
  (add [this k v] (swap! state assoc k v) this)
  (add-batch [this m] (swap! state merge m) this)
  (del [this k] (swap! state dissoc! k) this)
  (commit [this] (kv/insert-batch store (first (swap-vals! state (constantly {})))) this)
  (rollback [this] (reset! state {}) this))
