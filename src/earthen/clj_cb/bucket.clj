(ns earthen.clj-cb.bucket
  (:refer-clojure :exclude [get replace remove])
  (:import [com.couchbase.client.java Bucket]
           [com.couchbase.client.java.document JsonDocument]
           [com.couchbase.client.java.document.json JsonObject]
           [com.couchbase.client.java.error DocumentDoesNotExistException]
           [com.couchbase.client.java.query SimpleN1qlQuery Select]
           [com.couchbase.client.java.query.dsl Expression])
  (:require [clojure.data.json :as json]
            [earthen.clj-cb.utils :as u]))

(defn read-json
  "Reads a JSON value from input String.
  If data is nil, then nil is returned."
  [data]
  (when-not (nil? data) (json/read-json data true false "")))

(def ^{:doc "Wrapper of clojure.data.json/json-str."}
  write-json json/json-str)

(defn content->map
  "Returns the content of a JsonDocument as a map"
  [json-document]
  (if json-document
    (-> json-document
        .content
        .toString
        read-json)))

(defn document->map
  "Converts a JsonDocument to a map"
  [jsondocument]
  {:id (.id jsondocument)
   :cas (.cas jsondocument)
   :expiry (.expiry jsondocument)
   :mutation-token (.mutationToken jsondocument)
   :content (content->map jsondocument)})


(defn counter!
  "Increment or decrement a counter. If only key given, will take initial value as 0 (if not exists) and increment with 1"
  ([bucket k ]
   (counter! bucket k 1 0))
  ([bucket k delta initial]
   (-> (.counter bucket k delta initial)
       document->map)))

(defn get
  "Retrieves a document from the bucket. By default :json map document is returned.
  :raw is the string json"
  ([bucket id]
   (get bucket id :json))
  ([bucket id format]
   (let [doc (.get bucket id 5 (u/time :SECONDS))]
     (if doc
       (if (= :raw format)
         (-> doc
             .content
            .toString)
         (document->map doc))))))

(defn lookup-in
  "Retrieves sub-document values. To lookup particular objects / values in a document, pass a list of paths.
  Vectors in the response are converted from [n] to <n> as brackets are not valid in keywords. The
  DocumentFragment used to execute the request also has an .exists method that returns true or false for a
  given path. This has not been implemented. Instead you will get a {:keyword nil} response for a noneixstent path.
  If the document does not exist, an empty map {} is returned."
  [bucket id & rest]
  (try
    (let [builder (.lookupIn bucket id)
          _ (.get builder (into-array rest))
          result (.execute builder)]
      (apply merge-with into {}
             (map-indexed
              (fn [index item]
                {(keyword (clojure.string/replace item #"\[|\]" {"[" "<" "]" ">"}))
                 (if (contains? (supers (class (.content result index))) com.couchbase.client.java.document.json.JsonValue)
                   (read-json (.toString (.content result index)))
                   (.content result index))}) rest)))
    (catch DocumentDoesNotExistException ex
      {})))

(defn get-and-lock
  "Retrieves and locks the document for n seconds"
  [bucket id seconds]
  (let [doc (.getAndLock bucket id seconds)]
    (if doc
       (document->map doc))))

(defn touch!
  "Renews the expiration time of a document with the default key/value timeout"
  [bucket id expiry]
  (.touch bucket id expiry))

(defn get-and-touch!
  "Retrieve and touch a JsonDocument by its unique ID with the default key/value timeout."
  ([bucket id] (get-and-touch! bucket id 0))
  ([bucket id expiry]
   (document->map (.getAndTouch bucket id expiry))))

(defn unlock-document
  [bucket document]
  (.unlock bucket (:id document) (:cas document)))

(defn create-json-document
  "Creates a JsonDocument from a json-map"
  [id json-map]
  (let [json-object (JsonObject/fromJson (write-json json-map))]
    (JsonDocument/create id json-object)))

(defn replace!
  "Save or delete a document from the bucket"
  [bucket id json-map]
  (let [json (if (string? json-map) (read-json json-map) json-map)
        doc (create-json-document id json)]
    (document->map (.upsert bucket doc))))

(defn manager
  "Returns the manager giving a bucket"
  [bucket]
  (.bucketManager bucket))

(defn remove!
  "Remove the id from the bucket"
  [bucket id]
  (.remove bucket id))

(defn close
  "Closes the bucket connection"
  ([bucket] (close bucket 30 :SECONDS))
  ([bucket time type]
   (.close bucket time (u/time type))))

(defn create-primary-index
  "Create a primary index for bucket."
  [bucket]
  (.createN1qlPrimaryIndex (manager bucket) true false))

(defn query-rows->map
  "Convert JSON Iterator to a vector of maps."
  [result]
  (if (= "success" (.status result))
    (into [] (map #(read-json (.toString %)) (iterator-seq (.rows result))))
    []))

(defn simple-query->map
  "Converts a DefaultN1qlQueryResult to a map"
  [result]
  {:all-rows (.allRows result) 
   :context-id (.clientContextId result) 
   :errors(.errors result) 
   :final-success (.finalSuccess result)
   :n1ql-metrics (.info result) 
   :iterator (.iterator result) 
   :parse-success (.parseSuccess result) 
   :profile-info (.profileInfo result) 
   :requestid (.requestId result) 
   :rows (query-rows->map result) 
   :signature (.signature result) 
   :status (.status result)})

(defn query
  "Execute simple N1ql query."
  [bucket query-string]
  (simple-query->map (.query bucket (SimpleN1qlQuery/simple query-string))))

(defn statement
  "Prepare statement"
  [{:keys [select from where]}]
  (when (nil? select)
    (throw (ex-info "select missing" {})))

  (-> (Select/select (into-array select))
      (.from (Expression/i (into-array [from])))
      ;(println ,,, "blah")
      (as-> ,,, path
          (when where
            (println path)
            (doseq [clause where]
              (println "cl" clause)
              (when (keyword? clause)
                    (println "kw" clause))
              (let [{:keys [gt le]} clause]
                    
                    (when gt
                      (println "gt" gt))
                    (when le
                      (println "le" le)))
              
              )))

  ))

(class (statement {:select ["foo" "bar"] :from "foo" :where [{:gt ["99" "foo"]}
                                                             :and
                                                             {:le [100 "foo"]}]}))

(defn query-old
  "Execut N1ql query."
  [bucket query-string]
  (let [result (simple-query->map (.query bucket (SimpleN1qlQuery/simple query-string)))]
    (prn result)
    (if (= "success" (:status result)
      (into [] (map #(read-json (.toString % )) (iterator-seq (:rows result))))
      [])))
