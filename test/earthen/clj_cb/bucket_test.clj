(ns earthen.clj-cb.bucket-test
  (:require [clojure.test :refer :all]
            [earthen.clj-cb.bucket :as b]
            [earthen.clj-cb.utils :as u]
            [earthen.clj-cb.fixtures :as fx]))

(def book {:name "living-clojure"
           :year 2000
           :pages 12})

(def bigger-book {:name "bigger-living-clojure"
                  :year 2000
                  :pages 12
                  :editions {:2000 "1" :2001 "2"}
                  :publishers ["foo" "bar"]
                  :references [{:item "ref-item-0"}
                               {:item {:label 99}}]})


(use-fixtures :each fx/init)

(deftest crud
  (fx/authenticate "earthen" "earthen")
  (let [item (b/replace! (fx/bucket) (:name book) book)]
    (is (= book (:content item)) "insert/replace")
    (is (= item (b/get (fx/bucket) (:name book))))))

(deftest crud-authentication-fail
  (fx/authenticate "earthen" "notearthen")
  (is (thrown-with-msg?  com.couchbase.client.java.error.InvalidPasswordException
                         #"Passwords for bucket \"earthen_test\" do not match."
                         (b/replace! (fx/bucket) (:name book) book)))
  (is (thrown-with-msg?  com.couchbase.client.java.error.InvalidPasswordException
                         #"Passwords for bucket \"earthen_test\" do not match."
                         (b/get (fx/bucket) (:name book)))))

(deftest crud-get-fail
  (fx/authenticate "earthen" "earthen")
  (is (= nil (b/get (fx/bucket) "nonexistent-book"))))

(deftest lookup-in
  (fx/authenticate "earthen" "earthen")
  (let [item (b/replace! (fx/bucket) (:name bigger-book) bigger-book)]
    (is (= bigger-book (:content item)) "insert/replace")
    (is (= item (b/get (fx/bucket) (:name bigger-book))))
    (is (= {} (b/lookup-in (fx/bucket) "missing-id" "xyz")))
    (is (= {:editions.2000 "1"} (b/lookup-in (fx/bucket) (:name bigger-book) "editions.2000")))
    (is (= {:editions.2000 "1" :pages 12} (b/lookup-in (fx/bucket) (:name bigger-book) "editions.2000" "pages")))
    (is (= {:editions.2001 "2" :publishers ["foo" "bar"]} (b/lookup-in (fx/bucket) (:name bigger-book) "editions.2001" "publishers")))
    (is (= {:editions.2001 "2" :publishers ["foo" "bar"] :references<1>.item.label 99} (b/lookup-in (fx/bucket) (:name bigger-book) "editions.2001" "publishers" "references[1].item.label")))
    (is (= {:exists nil} (b/lookup-in (fx/bucket) (:name bigger-book) "exists")))
    (is (= {:exception "Path mismatch \"year.missing\" in bigger-living-clojure"} (b/lookup-in (fx/bucket) (:name bigger-book) "year.missing")))))

(deftest query
  (fx/authenticate "earthen" "earthen")
  (b/create-primary-index (fx/bucket))
  (dorun
   (map #(b/replace!
          (fx/bucket)
          (str (:name bigger-book) "-" %)
          (assoc bigger-book :name (str (:name bigger-book) "-" %)))
        (range 10)))
  (Thread/sleep 1000)
  (is (= 10 (count (:rows (b/query (fx/bucket) "SELECT meta().id, editions FROM `earthen_test`")))))
  (is (= 0 (count (:rows (b/query (fx/bucket) "SELECT * FROM `earthen_test` where name = \"not found\"")))))
  (is (= 1 (count (:rows (b/query (fx/bucket) "SELECT * FROM `earthen_test` where name = \"bigger-living-clojure-9\"")))))
  (let [result 
        (b/query (fx/bucket) "SELECT meta().id FROM `earthen_test` where name = \"bigger-living-clojure-7\"")]
    (is (= "success" (:status result)))
    (is (= 1 (count (:rows result))))
    (is (= "bigger-living-clojure-7" (:id (first (:rows result))))))
  (let [result 
        (b/query (fx/bucket) "SELECT meta().id FROM `earthen_test` LIMIT 3")]
    (is (= "success" (:status result)))
    (is (= 3 (count (:rows result))))
    (is (= "bigger-living-clojure-0" (:id (first (:rows result)))))
    (is (= "bigger-living-clojure-1" (:id (second (:rows result)))))
    (is (= "bigger-living-clojure-2" (:id (nth (:rows result) 2)))))
  (let [result 
        (b/query (fx/bucket) "SELECT meta().id FROM `earthen_test` OFFSET 3 LIMIT 3")]
    (is (= "success" (:status result)))
    (is (= 3 (count (:rows result))))
    (is (= "bigger-living-clojure-3" (:id (first (:rows result)))))
    (is (= "bigger-living-clojure-4" (:id (second (:rows result)))))
    (is (= "bigger-living-clojure-5" (:id (nth (:rows result) 2)))))
  (let [result 
        (b/query (fx/bucket) "SELECT meta().id FROM `earthen_test` OFFSET 9 LIMIT 1")]
    (is (= "success" (:status result)))
    (is (= 1 (count (:rows result))))
    (is (= "bigger-living-clojure-9" (:id (first (:rows result)))))
    )
  (let [result 
        (b/query (fx/bucket) "SELECT meta().id FROM `earthen_test` OFFSET 10 LIMIT 1")]
    (is (= "success" (:status result)))
    (is (= 0 (count (:rows result))))
    (is (= 0 (.resultCount (:n1ql-metrics result))))))

(deftest prepared-statement
  (is (= "SELECT foo FROM `bucket`" (.toString (b/statement {:select ["foo"]
                           :from "bucket"}))))
  (is (= "SELECT foo FROM `bucket` WHERE foo = $val"
         (.toString (b/statement {:select ["foo"]
                                  :from "bucket"
                                  :where [{:eq ["foo" "$val"]}]}))))
  (is (= "SELECT foo FROM `bucket` WHERE foo = $val1 OR foo != $val2"
         (.toString (b/statement {:select ["foo"]
                                  :from "bucket"
                                  :where [{:eq ["foo" "$val1"]}
                                          {:or {:ne ["foo" "$val2"]}}]}))))
  (is (= "SELECT foo FROM `bucket` WHERE foo = $val1 OR foo != $val2 LIMIT 10 OFFSET 10"
         (.toString (b/statement {:select ["foo"]
                                  :from "bucket"
                                  :where [{:eq ["foo" "$val1"]}
                                          {:or {:ne ["foo" "$val2"]}}]
                                  :limit 10
                                  :offset 10})))))

(deftest p-query
  (fx/authenticate "earthen" "earthen")
  (b/create-primary-index (fx/bucket))
  (let [item (b/replace! (fx/bucket) (:name bigger-book) bigger-book)]
    (dorun
     (map #(b/replace!
            (fx/bucket)
            (str (:name bigger-book) "-" %)
            (assoc bigger-book :name (str (:name bigger-book) "-" %)))
          (range 10)))
    (Thread/sleep 2000)
    (is (= 1 (count (:rows (b/query (fx/bucket) {:select ["*"]
                                                 :from "earthen_test"
                                                 :limit 1})))))
    (is (= 1 (count (:rows (b/query (fx/bucket) {:select ["*"]
                                                 :from "earthen_test"
                                                 :limit 1}
                                    (b/ad-hoc))))))
    (is (= 1 (count (:rows (b/p-query (fx/bucket) {:select ["pages"]
                                                   :from "earthen_test"
                                                   :where [{:eq ["name" "$title"]}]}
                                      {"title" "bigger-living-clojure"})))))
    (is (= 11 (count (:rows (b/p-query (fx/bucket) {:select ["meta().id" "pages"]
                                                   :from "earthen_test"
                                                   :where [{:like ["meta().id" ["bigger-%"]]}]}
                                      {"id" "bigger-living-clojure-0"})))))
    (is (= 1 (count (:rows (b/p-query (fx/bucket) {:select ["meta().id" "pages"]
                                                   :from "earthen_test"
                                                   :where [{:like ["meta().id" ["bigger-%"]]}
                                                           {:and {:gt ["meta().id" "$id"]}}]}
                                      {"id" "bigger-living-clojure-8"})))))
    (is (= 2 (count (:rows (b/p-query (fx/bucket) {:select ["meta().id" "pages"]
                                                   :from "earthen_test"
                                                   :where [{:like ["meta().id" ["bigger-%"]]}
                                                           {:and {:gt ["meta().id" "$id"]}}]
                                                   :order-by "meta().id"}
                                      {"id" "bigger-living-clojure-7"})))))
    (is (= 2 (count (:rows (b/p-query (fx/bucket) {:select ["meta().id" "pages"]
                                                   :from "earthen_test"
                                                   :use-index ["#primary"]
                                                   :where [{:like ["meta().id" ["bigger-%"]]}
                                                           {:and {:gt ["meta().id" "$id"]}}]
                                                   :order-by "meta().id"
                                                   :limit 2}
                                      {"id" "bigger-living-clojure-7"})))))))

;"SELECT meta().id, productIds from `%s` USE INDEX (`#primary`)
;                   WHERE meta().id LIKE 'ICustomerIndexDTO_%%' 
;                   AND meta().id > '%s' ORDER BY meta().id LIMIT %d"

(prn (.toString (into-array ["foo" "bar"])))

(into-array (second ["meta().id" ["bigger-%"]]))
