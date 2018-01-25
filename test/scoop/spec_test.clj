(ns scoop.spec-test
  (:require [scoop.spec :refer :all]
            [clojure.spec.alpha :as s]
            [clojure.test :refer :all]))

(def valid-create-topics-request {:version              0
                                  :request-type         :create-topics
                                  :timeout              40
                                  :create-topic-requests [{:topic              "test"
                                                           :num-partitions     1
                                                           :replication-factor 3
                                                           :replica-assignment []
                                                           :config-entries     []}]})
(def valid-api-versions-request {:version 1 :request-type :api-versions})

(def invalid-create-topics-request (assoc valid-create-topics-request :version 1))

(def non-existent-api-versions-request {:version 13 :request-type :api-versions})

(def non-existent-request-type {:version 1 :request-type :rogue-request})

(deftest validate-request-type-test
  (testing "valid request types are properly validated with the correct spec"
    (is (s/valid? :scoop.spec/valid-request valid-create-topics-request))
    (is (s/valid? :scoop.spec/valid-request valid-api-versions-request))
    (is (false? (s/valid? :scoop.spec/valid-request invalid-create-topics-request))))

  (testing "throws an exception if the spec for a request does not exist"
    (is (thrown? Exception (s/valid? :scoop.spec/valid-request non-existent-api-versions-request)))
    (is (thrown? Exception (s/valid? :scoop.spec/valid-request non-existent-request-type)))))
