(ns scoop.core
  (:require [scoop.protocol :as sp]))

;; Whats your name?

(-> {:version       3
     :request-type :metadata
     ;; nil value for topics means all topics
     :topics       nil}
    sp/send-request
    :cluster_id,)

;; Tell me more about your cluster members...

(-> {:version       3
     :request-type :metadata
     ;; nil value for topics means all topics
     :topics       nil}
    sp/send-request
    :brokers)

;; Which API version are you using?

(-> {:version       1
     :request-type :api-versions}
    sp/send-request
    :api_versions)


;; Who is the cluster controller?

(-> {:version       3
     :request-type :metadata
     ;; nil value for topics means all topics
     :topics       nil}
    sp/send-request
    :controller_id)


;; Who is the leader of a given partition and where are the replicas?

(-> {:version       3
     :request-type :metadata
     :topics        ["dogs"]}
    sp/send-request
    :topic_metadata)

;; Who is the coordinator of a given consumer group

(-> {:version      0
     :request-type :find-coordinator
     :group-id     "test19"}
    sp/send-request
    :coordinator)

;; Which topics have compaction as a clean.policy?

;; Which applications are using a given topic?

;; How many messages we have in a given topic?


;; What is the lag and current offset of a given consumer group?
(-> {:version      0
     :request-type :offset-fetch
     :group-id     "test"
     :topics       [{:topic "dogs" :partitions [{:partition 1}]}]}
    sp/send-request
    :responses)

;; this needs to be sent to the coordinator
(-> {:version      1
     :request-type :describe-groups
     :group-ids    ["test19"]}
    sp/send-request)

;; Do you have any topic without leader?

