(ns scoop.utils
  (:require [t6.from-scala.core :as $])
  (:import java.nio.ByteBuffer
           java.util.concurrent.TimeUnit
           [kafka.admin AdminClient AdminUtils]
           kafka.server.ConfigType
           kafka.utils.ZkUtils
           org.apache.kafka.common.Node
           org.apache.kafka.common.protocol.ApiKeys
           [org.apache.kafka.common.requests AbstractRequest AbstractRequest$Builder ApiVersionsRequest$Builder ControlledShutdownRequest$Builder JoinGroupRequest$Builder JoinGroupRequest$ProtocolMetadata LeaveGroupRequest$Builder ListGroupsRequest$Builder ListOffsetRequest$Builder MetadataRequest MetadataRequest$Builder]))

(defn pascal-case
  [s]
  (->> (clojure.string/split s #"-")
       (mapv clojure.string/capitalize)
       clojure.string/join))

(defn expose-private-method
  [target-class target-method]
  (let [methods (into [] (.getDeclaredMethods target-class))
        method  (first (filter #(= (.getName %) target-method) methods))
        _ (.setAccessible method true)]
    method))

(defn expose-private-field
  [target-class target-field]
  (let [field (.getDeclaredField target-class target-field)]
    (doto field
      (.setAccessible true))))


(def ^:private admin-client
  (AdminClient/createSimplePlaintext (or (System/getenv "BOOTSTRAP_SERVERS") "localhost:9092")))

;; TODO: Connection needs to be cleaned up if something goes wrong
(def ^:private network-client
  (.get (expose-private-field AdminClient "client") admin-client))

(defn send-to
  "Sends a request to an specific Kafka broker"
  [^AbstractRequest$Builder request ^Node node]
  (let [f (.send network-client node request)
        _ (.awaitDone f Long/MAX_VALUE TimeUnit/MILLISECONDS)]
    ;; TODO: This needs proper exception handling 
    (if (.succeeded f)
      (.. f value responseBody)
      (throw (.exception f)))))

(defn send-any-node
  "Sends a request to any Kafka broker"
  [^AbstractRequest$Builder request]
  (let [nodes ($/view (.findAllBrokers admin-client))
        node (rand-nth nodes)]
    (send-to request node)))

