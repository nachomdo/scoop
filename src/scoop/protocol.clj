(ns scoop.protocol
  (:require [clojure.spec.alpha :as s]
            [scoop.spec :as ss]
            [scoop.utils :as su])
  (:import java.lang.reflect.Modifier
           java.nio.ByteBuffer
           org.apache.kafka.common.Node
           org.apache.kafka.common.protocol.ApiKeys
           [org.apache.kafka.common.protocol.types ArrayOf BoundField Field Schema Struct Type Type$1 Type$10 Type$11 Type$12 Type$13 Type$2 Type$3 Type$4 Type$5 Type$6 Type$7 Type$8 Type$9]
           [org.apache.kafka.common.requests AbstractRequest AbstractRequest$Builder AbstractResponse]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; AbstractResponse translation into Clojure maps
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(declare struct->map map->struct)

(def ^:dynamic *version* (short 0))

(def types
  (clojure.set/map-invert {:boolean         Type$1
                           :int8            Type$2
                           :int16           Type$3
                           :int32           Type$4
                           :unsigned-int32  Type$5
                           :int64           Type$6
                           :string          Type$7
                           :nullable-string Type$8
                           :bytes           Type$9
                           :nullable-bytes  Type$10
                           :records         Type$11
                           :varint          Type$12
                           :varlong         Type$13
                           :array-of        ArrayOf
                           :schema          Schema}))

(defmulti unmarshall-field
  "Converts a field from its Kafka Type to Clojure"
  (fn [^BoundField f _]
    (let [field-type (.. f def type getClass)]
      (get types field-type :type-not-found))))

(defmethod unmarshall-field :boolean
  [^BoundField f ^Struct item]
  (.getBoolean item f))

(defmethod unmarshall-field :int8
  [^BoundField f ^Struct item]
  (.getByte item f))

(defmethod unmarshall-field :int16
  [^BoundField f ^Struct item]
  (.getShort item f))

(defmethod unmarshall-field :int32
  [^BoundField f ^Struct item]
  (.getInt item f))

(defmethod unmarshall-field :unsigned-int32
  [^BoundField f ^Struct item]
  (.getLong item f))

(defmethod unmarshall-field :int64
  [^BoundField f ^Struct item]
  (.getLong item f))

(defmethod unmarshall-field :string
  [^BoundField f ^Struct item]
  (.getString item f))

(defmethod unmarshall-field :nullable-string
  [^BoundField f ^Struct item]
  (.getString item f))

(defmethod unmarshall-field :bytes
  [^BoundField f ^Struct item]
  (.getBytes item f))

(defmethod unmarshall-field :nullable-bytes
  [^BoundField f ^Struct item]
  (.getBytes item f))

(defmethod unmarshall-field :records
  [^BoundField f ^Struct item]
  (.getRecords item f))

(defmethod unmarshall-field :varint
  [^BoundField f ^Struct item]
  (.getInt item f))

(defmethod unmarshall-field :varlong
  [^BoundField f ^Struct item]
  (.getLong item f))

(def simple-type?
  (let [simple-types (->> (.getDeclaredFields Type)
                          (filter #(Modifier/isStatic (.getModifiers %)))
                          (mapv #(format "ARRAY(%s)" (.getName %))))]
    (fn [^BoundField f]
      (some #{(.. f def type toString)} simple-types))))


(defmethod unmarshall-field :array-of
  [^BoundField field ^Struct item]
  (let [xs (.getArray item field)]
    (if (simple-type? field)
      (mapv identity xs)
      (mapv struct->map xs))))

(defn field-name
  "Extracts the field name"
  [^BoundField field]
  (.. field def name))


(defmethod unmarshall-field :schema
  [^BoundField f ^Struct item]
  (struct->map (.getStruct item (field-name f))))


(defn response->struct
  "Converts any AbstractResponse instance a Struct"
  [^AbstractResponse response]
  (.invoke (su/expose-private-method (class response) "toStruct")
           response
           (into-array Object [*version*])))

(defn struct->map
  "Converts any Struct instance into a Clojure map"
  [^Struct response]
  (let [fields (.. response schema fields)]
    (->> fields
         (reduce (fn [m field]
                   (assoc m
                          (field-name field)
                          (unmarshall-field field response)))
                 {})
         clojure.walk/keywordize-keys)))

(def response->map (comp struct->map response->struct))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Building AbstractRequest from Clojure maps
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn request-type->class
  [x]
  (->> x
       name
       su/pascal-case
       (format "org.apache.kafka.common.requests.%sRequest")
       symbol
       resolve))

(defn request-type-constructor
  "Finds the Struct based common constructor for
  a given request type"
  [x]
  (let [constructors (->> x
                         request-type->class
                         .getDeclaredConstructors
                         (into []))
        common-constructor? (fn [c]
                              (when (= (first (into [] (.getParameterTypes c)))
                                       org.apache.kafka.common.protocol.types.Struct)
                                c))]
    (some common-constructor? constructors)))

;; There is a better way using ApiKeys/schemaFor 
(defn request-type-schemas
  [x]
  (into [] (.invoke (.getDeclaredMethod (request-type->class x) "schemaVersions" nil) nil nil)))

(defn trace
  [x]
  (prn "trace " x)
  x)

(defn field-name->keyword
  [x]
  (keyword (clojure.string/replace x #"_" "-")))

(defn marshall-value
  [v]
  (cond
    (map? v) (map->struct v)
    (vector? v) (into-array Object (mapv marshall-value v))
    :else v))

(defn ^:private type-for-value
  [v]
  (cond (vector? v) (ArrayOf. (type-for-value (first v)))
        (string? v) Type/STRING
        (integer? v) Type/INT32
        (boolean? v) Type/BOOLEAN
        ;; Oversimplification. This could be a byte array as well 
        (nil? v) Type/NULLABLE_STRING 
        (bytes? v) Type/BYTES))

(defn keyword->field-name
  [k]
  (-> (name k)
      (clojure.string/replace #"-" "_")))

(defn map-entry->field
  [[k v]]
  (Field. (keyword->field-name k) (type-for-value v)))

(defn map->schema
  [m]
  (let [fields (mapv map-entry->field m)]
    (Schema. (into-array Field fields))))

(defn coerce
  [{:keys [version request-type] :as m}]
  (let [spec-name (format "%s-request-v%d" (name request-type) version)]
    (s/conform (keyword "scoop.spec" spec-name) m)))

(defn map->struct
  [{:keys [version request-type] :as m}]
  (let [schema (if request-type
                 (-> request-type request-type-schemas (nth version))
                 (map->schema m))
        fields (mapv field-name (.fields schema))
        unqualified-m (-> m
                          clojure.walk/stringify-keys
                          clojure.walk/keywordize-keys)]
    (reduce
     (fn [s v]
       (doto s
         (.set v (marshall-value ((field-name->keyword v) unqualified-m)))))
     (Struct. schema)
     fields)))

(defn map->request
  [{:keys [version request-type] :as m}]
  {:pre [(s/valid? ::ss/valid-request m)]}
  (let [ctr (request-type-constructor request-type)
        args [(map->struct (coerce m)) (short version)]]
    (.newInstance ctr (into-array Object args))))

(defn ^:private proxy-request-builder
  "ConsumerNetworkClient's send method expects an AbstractRequest.Builder
  but this class doesn't have a common constructor like AbstractRequest subclasses
  This function wraps our request type in an instance of AbstractRequest.Builder"
  [^AbstractRequest request ^ApiKeys api-key]
  (proxy [AbstractRequest$Builder] [api-key]
    (build
      ([this] request)
      ([this short] request))))

(defn ^:private request-type->api-key
  [request-type]
  (let [api-key (-> request-type keyword->field-name clojure.string/upper-case)
        field (su/expose-private-field ApiKeys api-key)]
    (.get field ApiKeys)))

(defn ^:private send-request*
  [{:keys [request-type version] :as m} send-fn]
  (binding [*version* (short version)]
    (-> m
        map->request
        (proxy-request-builder (request-type->api-key request-type))
        send-fn
        response->map)))

(defn send-request
  ([{:keys [request-type version] :as m}]
   {:pre [(s/valid? ::ss/valid-request m)]}
   (send-request* m su/send-any-node))
  ([{:keys [request-type version] :as m} ^Node node]
   {:pre [(s/valid? ::ss/valid-request m)]}
   (send-request* m #(su/send-to % node))))

(comment
  ;; Some examples that use the code above

  (send-request {:version 1 :request-type :api-versions})

  (send-request {:version 2  :request-type :metadata :topics nil})

  (defn kafka-node
    [id host port]
    (Node. id host port))

  (defn find-coordinator-for
    [group-id]
    (send-request {:version 1
                   :request-type :find-coordinator
                   :coordinator-type (byte 0)
                   :coordinator-key group-id}))

  ;; We can create a consumer with an specific group id with the command below:
  ;; kafka-console-consumer --consumer-property group.id=test19 --bootstrap-server localhost:9092 --topic my-topic --from-beginning
  (defn kill-consumer-group
    "Kills a client that belongs to the consumer group associated to the id"
    [group-id]
    (let [{:keys [node_id host port]} (:coordinator (find-coordinator-for group-id))]
      (send-request {:version 0
                     :request-type :join-group
                     :group-id group-id
                     :session-timeout 99999
                     :member-id ""
                     :protocol-type "consumer"
                     :group-protocols [{:protocol-name "range"
                                        :protocol-metadata (ByteBuffer/wrap (.getBytes ""))}
                                       {:protocol-name "roundrobin"
                                        :protocol-metadata (ByteBuffer/wrap (.getBytes ""))}]}
                    (kafka-node node_id host port))))

  (kill-consumer-group "my-group")

  )
