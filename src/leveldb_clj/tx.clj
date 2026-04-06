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

(def ^:private deleted ::deleted)

(deftype Binary [o]
  Object
  (equals [_ o2]
    (and (instance? Binary o2)
         (Arrays/equals ^bytes o ^bytes (raw o2))))
  (hashCode [_] (Arrays/hashCode ^bytes o))
  (toString [_] (str o))
  IComparable
  (equals [_ o1 o2]
    (and (instance? Binary o1)
         (instance? Binary o2)
         (Arrays/equals ^bytes (raw o1) ^bytes (raw o2))))
  IConvertable
  (raw [_] o))

(defn- commit-state [store state]
  (let [puts (reduce-kv
               (fn [acc k v]
                 (if (= deleted v)
                   acc
                   (assoc acc (.raw k) (.raw v))))
               {}
               state)
        deletes (keep (fn [[k v]]
                        (when (= deleted v)
                          (.raw k)))
                      state)]
    (doseq [k deletes]
      (kv/delete store k))
    (when (seq puts)
      (kv/insert-batch store puts))))

(defrecord LevelDB-TX [store state]
  ITransactional
  (add [this k v] (swap! state assoc (->Binary k) (->Binary v)) this)
  (add-batch [this m]
    (swap! state merge (into {} (map (fn [[k v]] [(->Binary k) (->Binary v)]) m)))
    this)
  (del [this k] (swap! state assoc (->Binary k) deleted) this)
  (commit [this]
    (commit-state store (first (swap-vals! state (constantly {}))))
    this)
  (rollback [this] (reset! state {}) this))
