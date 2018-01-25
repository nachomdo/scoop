(ns scoop.spec
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]))

;; Conformers to coerce numerical values to the proper type

(def int8 (s/conformer #(if (and (int? %) (< % 256))
                          (byte %)
                          ::s/invalid)))

(def int16 (s/with-gen (s/conformer #(if (and (int? %) (< % 32,768))
                            (short %)
                            ::s/invalid))
             #(gen/int)))

(def int32 (s/with-gen (s/conformer #(if (and (int? %) (<= % Integer/MAX_VALUE))
                            (int %)
                            ::s/invalid))
             #(gen/int)))


(s/def ::request-type
  #{:produce :fetch :list-offsets :metadata
    :offset-commit :find-coordinator :join-group :offset-fetch
    :heartbeat :leave-group :sync-group :stop-replica
    :controlled-shutdown :update-metadata :leader-and-isr
    :describe-groups :list-groups :api-versions :create-topics
    :delete-topics :delete-records :init-producer-id :offset-for-leader-epoch
    :add-partitions-to-txn :add-offsets-to-txn :end-txn :write-txn-markers
    :txn-offset-commit :describe-acls :create-acls :delete-acls
    :describe-configs :alter-configs :alter-replica-log-dirs :create-partitions})

;; Common specs for AbstractRequest

(s/def ::version (s/with-gen (s/conformer #(if (s/int-in-range? 0 6 %)
                                  (short %)
                                  ::s/invalid))
                   #(gen/int)))

(s/def ::request (s/keys :req-un [::version ::request-type]))

;; ApiVersionsRequest specs

(s/def ::api-versions-request-v0 (s/merge ::request))

;; Same that v0 but Throttle time has been added to response
(s/def ::api-versions-request-v1 (s/merge ::request))

;; MetadataRequest specs

(s/def ::topic string?)

(s/def :metadata-request-v0/topics (s/coll-of ::topic))
(s/def ::metadata-request-v0 (s/merge ::request (s/keys :req-un [:metadata-request-v0/topics])))

(s/def :metadata-request-v1/topics (s/nilable :metadata-request-v0/topics))
(s/def ::metadata-request-v1 (s/merge ::request (s/keys :req-un [:metadata-request-v1/topics])))

;; Same that v1. An additional field for cluster id has been added to the v2 metadata response
(s/def ::metadata-request-v2 (s/merge ::metadata-request-v1))
;; Same that v1 and v2. An additional field for throttle time has been added to the v3 metadata response
(s/def ::metadata-request-v3 (s/merge ::metadata-request-v2))

(s/def ::allow-auto-topic-creation boolean?)
(s/def ::metadata-request-v4 (s/merge ::metadata-request-v3 (s/keys :req-un [::allow-auto-topic-creation])))

;; Same that v4. An additional field for offline_replicas has been added to the v5 metadata response
(s/def ::metadata-request-v5 (s/merge ::metadata-request-v4))

;; ListGroupsRequest specs

(s/def ::list-groups-request-v0 (s/merge ::request))
;; Same that v0 but Throttle time has been added to response
(s/def ::list-groups-request-v1 (s/merge ::list-groups-request-v0))

;; DescribeGroupRequest specs
(s/def ::group-id string?)
(s/def ::group-ids (s/coll-of ::group-id))

(s/def ::describe-groups-request-v0 (s/merge ::request (s/keys :req-un [::group-ids])))
;; Same that v0 but Throttle time has been added to response
(s/def ::describe-groups-request-v1 (s/merge ::describe-groups-request-v0))


;; CreateTopicsRequest specs
(s/def ::num-partitions int32)
(s/def ::replication-factor int16)
(s/def ::partition int32)
(s/def ::replica int32)
(s/def ::replicas (s/coll-of ::replica))
(s/def ::partition-replica-assignment-entry (s/keys :req-un [::partition ::replicas]))
(s/def ::replica-assignment (s/coll-of ::partition-replica-assignment-entry))
(s/def ::config-name string?)
(s/def ::config-value (s/nilable string?))
(s/def ::config-entry (s/keys :req-un [::config-name ::config-value]))
(s/def ::config-entries (s/coll-of ::config-entry))
(s/def ::timeout int32)

(s/def :create-topics-request-v0/topic (s/keys :req-un [::topic
                                                        ::num-partitions
                                                        ::replication-factor
                                                        ::replica-assignment
                                                        ::config-entries]))
(s/def ::create-topic-requests (s/with-gen
                                 (s/coll-of :create-topics-request-v0/topic)
                                #(gen/not-empty (gen/vector (s/gen :create-topics-request-v0/topic))))) 

(s/def ::create-topics-request-v0 (s/merge ::request (s/keys :req-un [::create-topic-requests
                                                                      ::timeout])))

(s/def ::validate-only boolean?)
(s/def ::create-topics-request-v1 (s/merge ::request
                                           (s/keys :req-un [::create-topic-requests
                                                            ::timeout
                                                            ::validate-only])))
;; same as v1. Throttle time has been added to the response
(s/def ::create-topics-request-v2 (s/spec ::create-topics-request-v1))

;; OffsetFetchRequest specs
(s/def ::partitions (s/coll-of (s/keys :req-un [::partition])))
(s/def :offset-fetch-request-v0/topics (s/coll-of (s/keys :req-un [::topic ::partitions])))

(s/def :offset-fetch-request-v2/topics (s/with-gen (s/nilable (s/coll-of (s/keys :req-un [::topic ::partitions])))
                                         #(gen/not-empty (gen/vector (s/gen (s/keys :req-un [::topic ::partitions]))))))

;; Reads offsets from ZK
(s/def ::offset-fetch-request-v0 (s/merge ::request (s/keys :req-un [::group-id :offset-fetch-request-v0/topics])))
;; Reads offsets from Kafka
(s/def ::offset-fetch-request-v1 (s/merge ::offset-fetch-request-v0))
(s/def ::offset-fetch-request-v2 (s/merge ::request (s/keys :req-un [::group-id :offset-fetch-request-v2/topics])))
;; same as v2. Throttle time has been added to v3 response
(s/def ::offset-fetch-request-v3 (s/merge ::offset-fetch-request-v2))

;; JoinGroupRequest specs
(s/def ::protocol-name #{"range" "roundrobin"})
(s/def ::protocol-metadata (s/with-gen #(instance? Object %)
                             #(gen/bytes)))

(s/def ::join-group-request-protocol (s/keys :req-un [::protocol-name ::protocol-metadata]))
(s/def ::session-timeout int32)
(s/def ::member-id string?)
(s/def ::protocol-type #{"consumer"})
(s/def ::group-protocols (s/coll-of ::join-group-request-protocol))
(s/def ::join-group-request-v0 (s/merge ::request (s/keys :req-un [::group-id
                                                                   ::session-timeout
                                                                   ::member-id
                                                                   ::protocol-type
                                                                   ::group-protocols])))

(s/def ::rebalance-timeout int32)
(s/def ::join-group-request-v1 (s/merge ::request (s/keys :req-un [::group-id
                                                                   ::session-timeout
                                                                   ::rebalance-timeout
                                                                   ::member-id
                                                                   ::protocol-type
                                                                   ::group-protocols])))
;; v2 request is the same as v1. Throttle time has been added to response 
(s/def ::join-group-request-v2 (s/merge ::join-group-request-v1))

;; FindCoordinatorRequest specs
(s/def ::find-coordinator-request-v0 (s/merge ::request (s/keys :req-un [::group-id])))
(s/def ::coordinator-key string?)
;; 0 group coordinator, 1 transaction coordinator
(s/def ::coordinator-type #{(byte 0) (byte 1)})

(s/def ::find-coordinator-request-v1 (s/merge ::request (s/keys :req-un [::coordinator-type ::coordinator-key])))


;; ¯\_(ツ)_/¯ it needs to be a multi-method due to s/multi-spec
(defmulti fetch-spec 
  "allows to choose a spec based on version and request-type"
  (fn [x] :default))

(defmethod fetch-spec :default
  [{:keys [request-type version] :as x}]
  (keyword (format "scoop.spec/%s-request-v%d" (name request-type) version)))

(s/def ::valid-request (s/multi-spec fetch-spec :valid-request))
