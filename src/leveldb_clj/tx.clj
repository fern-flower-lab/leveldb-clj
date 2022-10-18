(ns leveldb-clj.tx
  (:gen-class)
  (:require [leveldb-clj.kv :as kv])
  (:import (java.util Arrays)))

(defprotocol ITransactional
  (add [_ k v] "Add k->v pair to transaction")
  (add-batch [_ m] "Add multiple k->v pairs to transaction")
  (del [_ k] "Delete k->v pair from transaction")
  (commit [_] "Commit transaction (push prepared data to the store)")
  (rollback [_] "Drop yet not stored data from the internal state (reset)"))

(defprotocol IComparable (equals [_ o1 o2]))
(defprotocol IConvertable (raw [_]))

(deftype Binary [o]
  Object
  (equals [_ o2] (Arrays/equals ^bytes o ^bytes (raw o2)))
  (hashCode [_] (.hashCode o))
  (toString [_] (str o))
  IComparable
  (equals [_ o1 o2] (Arrays/equals ^bytes (raw o1) ^bytes (raw o2)))
  IConvertable
  (raw [_] o))

(defn- extract-state [[state _]]
  (into {} (map (fn [[k v]] [(.raw k) (.raw v)]) state)))

(defrecord LevelDB-TX [store state]
  ITransactional
  (add [this k v] (swap! state assoc (->Binary k) (->Binary v)) this)
  (add-batch [this m] (swap! state merge m) this)
  (del [this k] (swap! state dissoc (->Binary k)) this)
  (commit [this] (kv/insert-batch store (extract-state (swap-vals! state (constantly {})))) this)
  (rollback [this] (reset! state {}) this))
