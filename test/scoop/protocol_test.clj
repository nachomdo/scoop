(ns scoop.protocol-test
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.test :refer :all]
            [scoop.protocol :refer :all]
            [clojure.test.check.clojure-test :as ct]
            [clojure.test.check.properties :as prop]
            [scoop.spec :as ss])
  (:import [org.apache.kafka.common.requests AbstractRequest MetadataRequest]))
 
(defn request-type-and-version
  [x]
  (->> x
       name
       (re-find #"([\w|\\-]+)-request-v(\d+)")
       rest))

(defn adjust-generator-for
  [spec]
  (let [[req-type version] (request-type-and-version spec)]
    (gen/fmap #(assoc %
                      :version (read-string version)
                      :request-type (keyword req-type))
              (s/gen spec))))

(def gen-requests
  (gen/vector
   (gen/one-of (mapv adjust-generator-for [::ss/create-topics-request-v0 ::ss/create-topics-request-v1 ::ss/create-topics-request-v2
                                           ::ss/metadata-request-v0 ::ss/metadata-request-v1 ::ss/metadata-request-v3 ::ss/metadata-request-v4
                                           ::ss/metadata-request-v5 ::ss/list-groups-request-v0 ::ss/list-groups-request-v1
                                           ::ss/offset-fetch-request-v0 ::ss/offset-fetch-request-v1 ::ss/offset-fetch-request-v2
                                           ::ss/offset-fetch-request-v3 ::ss/api-versions-request-v0 ::ss/api-versions-request-v1
                                           ::ss/find-coordinator-request-v0 ::ss/find-coordinator-request-v1
                                           ::ss/join-group-request-v0 ::ss/join-group-request-v1 ::ss/join-group-request-v2]))))
 
(ct/defspec map->request-test 25
  (prop/for-all [xs (gen/not-empty gen-requests)]
                (every? #(instance? AbstractRequest (map->request %)) xs)))

(ct/defspec send-test 10
  (prop/for-all [xs (gen/not-empty gen-requests)]
                (doseq [x xs]
                  (send-request x))
                true))
