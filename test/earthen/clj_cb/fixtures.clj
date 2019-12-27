(ns earthen.clj-cb.fixtures
  (:require [clojure.test :refer :all]
            [earthen.clj-cb.cluster :as c]))

(def cluster (atom nil))

(def bucket-name "earthen_test")
(def default-bucket-settings {:name bucket-name
                              :type :COUCHBASE
                              :quota 100
                              :port 0
                              :password ""
                              :replicas 0
                              :index-replicas false
                              :flush? true})

(defn bucket
  []
  (c/open-bucket @cluster bucket-name))

(defn manager
  []
  (c/manager @cluster {:username "Administrator" :password "Admin123"}))

;; (defn init-bucket
;;   [f]
;;   (c/remove-bucket! manager bucket-name)
;;   (let [create-bucket (c/insert-bucket! manager default-bucket-settings)
;;         get-bucket (fn [name] (->> (c/buckets manager) (some #(if (= (:name %) name) %))))
;;         manager (c/manager cluster {:username "earthen" :password "earthen"})]
;;     (f)))

(defn authenticate
  [username password]
  (c/authenticate @cluster username password))

(defn- get-cluster-from-env
  "Used in github action where port is defined in job.services.couchbase.ports['11210']"
  []
  (let [http-port (System/getenv "HTTP_DIRECT_PORT")
        carrier-port (System/getenv "CARRIER_DIRECT_PORT")]
    (System/setProperty "com.couchbase.disconnectTimeout" "60000")
    (if (some? http-port)
      (do
        (println "Setting " http-port carrier-port)
        (System/setProperty "com.couchbase.NetworkResolution" "external")
        (System/setProperty "com.couchbase.bootstrapHttpEnabled" "true")
        (System/setProperty "com.couchbase.bootstrapHttpDirectPort" http-port)
        (System/setProperty "com.couchbase.bootstrapCarrierDirectPort" carrier-port)
        "127.0.0.1")
      "127.0.0.1")))


(defn init
  [f]
  (let [host (get-cluster-from-env)]
    (prn "HOSTNAME is: " host)
    (if (some? host)
      (reset! cluster (c/create host))
      (reset! cluster (c/create))))
  (c/remove-bucket! (manager) bucket-name)
  (c/insert-bucket! (manager) default-bucket-settings)
  (f)
  (try
    (c/disconnect @cluster)
    (catch java.util.concurrent.RejectedExecutionException e (println (str "Caught Expected Exception " e)))))

