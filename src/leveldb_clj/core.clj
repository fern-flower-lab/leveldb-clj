(ns leveldb-clj.core
  (:require [leveldb-clj.kv :as kv]
            [clojure.java.io :as java.io])
  (:import (org.iq80.leveldb.impl Iq80DBFactory)
           (org.iq80.leveldb Options WriteBatch DBIterator)
           (java.io Closeable)))

(defprotocol LevelDBCodec
  (outgoing-key [_ k])
  (incoming-key [_ k])
  (outgoing-value [_ v])
  (incoming-value [_ v]))

(extend-type nil
  LevelDBCodec
  (outgoing-key [_ k] k)
  (incoming-key [_ k] k)
  (outgoing-value [_ v] v)
  (incoming-value [_ v] v))

(deftype CloseableIteratorSeq [^DBIterator iterator seq-val]
  clojure.lang.Seqable
  (seq [_] seq-val)
  clojure.lang.ISeq
  (first [_] (first seq-val))
  (next [_] (next seq-val))
  (more [_] (rest seq-val))
  (cons [_ o] (cons o seq-val))
  Closeable
  (close [_] (.close iterator)))

(defn- closeable-iterator-seq
  "Returns a Closeable sequence backed by the iterator.
   Caller should close when done, e.g., with `with-open`."
  [^DBIterator it]
  (.seekToFirst it)
  (->CloseableIteratorSeq it (iterator-seq it)))

(defrecord LevelDBStore [db codec]
  kv/KeyValueStore
  (insert [this k v]
    (.put db (outgoing-key codec k) (outgoing-value codec v))
    this)
  (insert-batch [this m]
    (let [batch ^WriteBatch (.createWriteBatch db)]
      (try
        (doseq [[k v] m]
          (.put batch (outgoing-key codec k) (outgoing-value codec v)))
        (.write db batch)
        (finally
          (.close batch))))
    this)
  (retrieve [this k]
    (incoming-value codec (.get db (outgoing-key codec k))))
  (delete [this k]
    (.delete db (outgoing-key codec k))
    this)
  (list-keys [this]
    (with-open [s (kv/stream this)]
      (doall (map #(.getKey %) s))))
  (stream [this]
    (closeable-iterator-seq (.iterator db)))
  Closeable
  (close [_] (.close db)))

(defn map->LevelDBStore [opts]
  (let [options (doto (Options.)
                  (.createIfMissing true))
        f (java.io/file (:path opts))
        db (.open Iq80DBFactory/factory f options)]
    (LevelDBStore. db (:codec opts))))
