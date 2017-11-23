(ns sparkling.dataset-test
  (:require [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [camel-snake-kebab.extras :refer [transform-keys]]
            [clojure.test :refer :all]
            [clojure.java.io :as io]
            [sparkling.dataset :as sut]
            [sparkling.sql.types :as types]
            [clojure.test :as t]))

(def ^:dynamic *session*)

(defn with-session
  [t]
  (with-open [session (sut/spark-session {:app-name "unit-test"})]
    (binding [*session* session]
      (t))))

(use-fixtures :each with-session)

(deftest read-json-test
  (let [ds (sut/read-json *session* (io/resource "euclid/elements.txt"))]
    (is (= #{"name" "description" "references"} (set (sut/columns ds))))
    (is (= 606 (sut/count ds)))))

(def cn1 {"name"        "C.N.1"
          "description" "Things which are equal to the same thing are also equal to one another."})

(deftest select-where-test
  (testing "select columns by name with an SQL where expression"
    (let [result (-> (sut/read-json *session* (io/resource "euclid/elements.txt"))
                     (sut/select-columns ["name" "description"])
                     (sut/where-expr "name = 'C.N.1'"))
          row    (sut/first result)]
      (is (= 1 (sut/count result)))
      (is (= cn1 (sut/row->map row)))
      (is (= (transform-keys ->kebab-case-keyword cn1) (sut/row->map row true)))))

  (testing "select columns by column expression with a column condition for the where clause"
    (let [ds        (sut/read-json *session* (io/resource "euclid/elements.txt"))
          condition (.equalTo (sut/col ds "name") "C.N.1")
          cols      (sut/cols ds ["name" "description"])
          result    (-> ds
                        (sut/select-columns cols)
                        (sut/where condition))
          row       (sut/first result)]
      (is (= 1 (sut/count result)))
      (is (= cn1 (sut/row->map row)))
      (is (= (transform-keys ->kebab-case-keyword cn1) (sut/row->map row true))))))

(deftest select-group-agg-test
  (let [ds (-> (sut/read-json *session* (io/resource "euclid/elements.txt"))
               (sut/select-expr ["explode(references) as reference"])
               (sut/group-by ["reference"])
               (.count)
               (sut/with-column-renamed "count" "referenced"))
        sort-col (.desc (sut/col ds "referenced"))
        result (-> ds
                   (sut/order-by-exprs [sort-col])
                   (sut/first)
                   (sut/row->map true))]
    (is (= {:reference "Prop.6.1" :referenced 87} result))))
