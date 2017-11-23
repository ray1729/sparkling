(ns sparkling.dataset
  "Spark dataset API for Clojure."
  (:refer-clojure :exclude [alias count distinct first group-by sort take])
  (:require [camel-snake-kebab
             [core :refer [->camelCaseString ->snake_case_string ->SCREAMING_SNAKE_CASE_STRING
                           ->kebab-case-keyword]]
             [extras :refer [transform-keys]]]
            [sparkling.destructuring :as s-de]
            [sparkling.utils :as u]
            [sparkling.sql.types :as types])
  (:import  [org.apache.spark.sql SparkSession Dataset Row Column SaveMode]
            [org.apache.spark.sql.types StructType]
            [org.apache.spark.storage StorageLevel]))

(defn- apply-config
  [builder config]
  (reduce-kv (fn [builder config-key config-val]
               (.config builder config-key config-val))
             builder
             config))

(defn- xor [a b] (and (or a b) (not (and a b))))

(defn spark-session
  [{:keys [master app-name enable-hive config] :or {master "local[*]"}}]
  {:pre [(some? master) (some? app-name)]}
  (cond-> (SparkSession/builder)
    master      (.master master)
    app-name    (.appName app-name)
    config      (apply-config config)
    enable-hive (.enableHiveSupport)
    true        (.getOrCreate)))

(defn spark-context
  [^SparkSession spark-session]
  (.sparkContext spark-session))

(defn read-parquet
  [^SparkSession session & args]
  (let [[[options] paths] (split-with map? args)]
    (cond-> (.read session)
      options (.options (transform-keys ->camelCaseString options))
      true    (.parquet (into-array String (map str paths))))))

(defn read-json
  [^SparkSession session & args]
  (let [[[options] paths] (split-with map? args)]
    (cond-> (.read session)
      options (.options (transform-keys ->camelCaseString options))
      true    (.json (into-array String (map str paths))))))

(def save-mode
  {:overwrite SaveMode/Overwrite
   :append    SaveMode/Append
   :ignore    SaveMode/Ignore
   :error     SaveMode/ErrorIfExists})

(defn write-parquet
  [^Dataset dataset path & [{:keys [mode partition-by sort-by options]}]]
  {:pre [(some? path) (or (nil? mode) (save-mode mode))]}
  (cond-> (.write dataset)
    mode         (.mode (save-mode mode))
    partition-by (.partitionBy (into-array String partition-by))
    sort-by      (.sortBy (into-array String sort-by))
    options      (.options (transform-keys ->camelCaseString options))
    true         (.parquet (str path))))

(defn coll->data-frame
  "Create a Dataframe from a Clojure collection. Each member of the
  collection must be a vector that conforms to the specified schema."
  [^SparkSession session ^StructType schema coll]
  (.createDataFrame session (java.util.ArrayList. (map types/create-row coll)) schema))

(defn rdd->data-frame
  "Create a Dataframe from a Row RDD."
  [^SparkSession session ^StructType schema rdd]
  (.createDataFrame session rdd schema))

(defn agg
  "Aggregates on the entire dataset without groups."
  [^Dataset dataset exprs]
  {:pre [(map? exprs) (every? string? (keys exprs)) (every? string? (vals exprs))]}
  (.agg dataset exprs))

(defn alias
  "Returns a new Dataset with an alias set."
  [^Dataset dataset ^String alias]
  (.alias dataset alias))

(defn cache
  "Persist this Dataset with the default storage level (MEMORY_AND_DISK)."
  [^Dataset dataset]
  (.cache dataset))

(defn checkpoint
  "(Eagerly) checkpoint a Dataset and return the new Dataset."
  ([^Dataset dataset]
   (.checkpoint dataset))
  ([^Dataset dataset eager?]
   (.checkpoint dataset eager?)))

(defn coalesce
  "Returns a new Dataset that has exactly num-partitions partitions."
  [^Dataset dataset num-partitions]
  (.coalesce dataset num-partitions))

(defn col
  "Selects column based on the column name and return it as a Column."
  [^Dataset dataset ^String column-name]
  (.col dataset column-name))

(defn cols
  "Selects columns based on the column names, return a seq of Columns."
  [^Dataset dataset column-names]
  (map (partial col dataset) column-names))

(defn collect
  "Returns a sequence that contains all of Rows in this Dataset."
  [^Dataset dataset]
  (seq (.collectAsList dataset)))

(defn columns
  "Returns all column names as a sequence."
  [^Dataset dataset]
  (seq (.columns dataset)))

(defn count
  "Returns the number of rows in the dataset"
  [^Dataset dataset]
  (.count dataset))

(defn create-global-temp-view
  "Creates a global temporary view using the given name."
  [^Dataset dataset ^String view-name]
  (.createGlobalTempView dataset view-name))

(defn create-or-replace-temp-view
  "Creates a local temporary view using the given name."
  [^Dataset dataset ^String view-name]
  (.createOrReplaceTempView dataset view-name))

(defn create-temp-view
  "Creates a local temporary view using the given name."
  [^Dataset dataset ^String view-name]
  (.createTempView dataset view-name))

(defn cross-join
  "Explicit cartesian join with another Dataset."
  [^Dataset dataset ^Dataset right]
  (.crossJoin dataset right))

(defn cube
  "Create a multi-dimensional cube for the current Dataset using the
  specified columns, so we can run aggregation on them."
  [^Dataset dataset column-name & column-names]
  (.cube dataset column-name (into-array String column-names)))

(defn describe
  "Computes statistics for numeric and string columns, including
  count, mean, stddev, min, and max."
  [^Dataset dataset & column-names]
  {:pre [(seq column-names)]}
  (.describe dataset (into-array String column-names)))

(defn distinct
  "Returns a new Dataset that contains only the unique rows from this Dataset."
  [^Dataset dataset]
  (.distinct dataset))

(defn drop-columns
  "Returns a new Dataset with columns dropped."
  [^Dataset dataset & column-names]
  {:pre [(seq column-names)]}
  (.drop dataset (into-array String column-names)))

(defn drop-duplicates
  "Returns a new Dataset with duplicate rows removed, considering only the subset of columns.
  (See `distinct` for a variant that considers all columns.)"
  ([^Dataset dataset & column-names]
   {:pre [(seq column-names)]}
   (.dropDuplicates dataset (into-array String column-names))))

(defn dtypes
  "Returns all column names and their data types."
  [^Dataset dataset]
  (map (s-de/key-value-fn vector) (.dtypes dataset)))

(defn except
  "Returns a new Dataset containing rows in this Dataset but not in another Dataset."
  [^Dataset dataset ^Dataset other]
  (.except dataset other))

(defn explain
  "Prints the physical plan to the console for debugging purposes."
  ([^Dataset dataset]
   (.explain dataset))
  ([^Dataset dataset extended?]
   (.explain dataset extended?)))

(defn filter-col
  "Filters rows using the given column condition."
  [^Dataset dataset ^Column condition]
  (.filter dataset condition))

;; XXX TODO: filter-pred, filter on FilterFunction<T> predicate function.

(defn filter-sql
  "Filters rows using the given SQL expression."
  [^Dataset dataset ^String condition-expr]
  (.filter dataset condition-expr))

(defn first
  "Returns the first row."
  [^Dataset dataset]
  (.first dataset))

;; XXX TODO: flat-map ?

(defn group-by
  "Groups the Dataset using the specified columns, so that we can run aggregation on them."
  [^Dataset dataset column-names]
  {:pre [(seq column-names) (every? string? column-names)]}
  (.groupBy dataset (clojure.core/first column-names) (into-array String (rest column-names))))

(defn head
  "Returns the first n rows."
  [^Dataset dataset n]
  {:pre [(> n 0)]}
  (.head dataset n))

(defn input-files
  "Returns a best-effort snapshot of the files that compose this Dataset."
  [^Dataset dataset]
  (seq (.inputFiles dataset)))

(defn intersect
  "Returns a new Dataset containing rows only in both this Dataset and another Dataset."
  [^Dataset dataset ^Dataset other]
  (.intersect dataset other))

(defn local?
  "Returns true if the collect and take methods can be run
  locally (without any Spark executors)."
  [^Dataset dataset]
  (.isLocal dataset))

(defn streaming?
  "Returns true if this Dataset contains one or more sources that
  continuously return data as it arrives."
  [^Dataset dataset]
  (.isStreaming dataset))

(defn java-rdd
  "Returns the content of the dataset as a JavaRDD"
  [^Dataset dataset]
  (.javaRDD dataset))

(defn join
  "Join with another DataFrame."
  ([^Dataset dataset ^Dataset right & {:keys [expr type using] :or {type :inner}}]
   {:pre [(xor expr using)
          (contains? #{:inner :outer :left-outer :right-outer :leftsemi :leftanti} type)]}
   (.join dataset right (or expr using) (->snake_case_string type))))

(defn limit
  "Returns a new Dataset by taking the first n rows."
  [^Dataset dataset n]
  {:pre [(>= n 0)]}
  (.limit dataset n))

;; XXX TODO: map, map-partitions

(defn order-by
  "Returns a new Dataset sorted by the given columns."
  [^Dataset dataset column-names]
  {:pre [(seq column-names) (every? string? column-names)]}
  (.orderBy dataset (clojure.core/first column-names) (into-array String (rest column-names))))

(defn order-by-exprs
  "Returns a new Dataset sorted by the given column expressions."
  [^Dataset dataset exprs]
  {:pre [(seq exprs)]}
  (.orderBy dataset (into-array Column exprs)))

(defn storage-level
  "Convert keyword (or string) to a StorageLevel."
  [level]
  (StorageLevel/fromString (->SCREAMING_SNAKE_CASE_STRING level)))

(defn persist
  "Persist this Dataset with the given storage level (default MEMORY_AND_DISK)."
  ([^Dataset dataset]
   (.persist dataset))
  ([^Dataset dataset level]
   (.persist dataset (storage-level level))))

(defn print-schema
  "Prints the schema to the console in a nice tree format."
  [^Dataset dataset]
  (.printSchema dataset))

(defn random-split
  "Randomly splits this Dataset with the provided weights."
  ([^Dataset dataset weights]
   (.randomSplit dataset (into-array Double weights)))
  ([^Dataset dataset weights seed]
   (.randomSplit dataset (into-array Double weights) seed)))

(defn random-split-seq
  "Returns a sequence that contains randomly split Dataset with the provided weights."
  [^Dataset dataset weights seed]
  (seq (.randomSplitAsList dataset (into-array Double weights) seed)))

(defn rdd
  "Represents the content of the Dataset as an RDD."
  [^Dataset dataset]
  (.rdd dataset))

;; XXX TODO: reduce

(defn repartition
  "Returns a new Dataset partitioned by the given partitioning
  expressions into num-partitions."
  ([^Dataset dataset num-partitions]
   (.repartition dataset num-partitions))
  ([^Dataset dataset num-partitions & partition-exprs]
   (.repartition dataset num-partitions (into-array Column partition-exprs))))

(defn rollup
  "Create a multi-dimensional rollup for the current Dataset using the
  specified columns, so we can run aggregation on them."
  [^Dataset dataset & column-names]
  {:pre [(seq column-names)]}
  (.rollup dataset (into-array String column-names)))

(defn sample
  "Returns a new Dataset by sampling a fraction of rows, using a
  user-supplied seed."
  ([^Dataset dataset ^Boolean with-replacement? ^Double fraction]
   (.sample dataset with-replacement? fraction))
  ([^Dataset dataset ^Boolean with-replacement? ^Double fraction ^Long seed]
   (.sample dataset with-replacement? fraction seed)))

(defn schema
  [^Dataset dataset]
  (.schema dataset))

(defn select-columns
  "Selects a set of columns."
  [^Dataset dataset columns]
  {:pre [(seq columns)]}
  (if (string? (clojure.core/first columns))
    (.select dataset (clojure.core/first columns) (into-array String (rest columns)))
    (.select dataset (into-array Column columns))))

(defn select-expr
  "Selects a set of SQL expressions."
  [^Dataset dataset sql-exprs]
  {:pre [(seq sql-exprs)]}
  (.selectExpr dataset (into-array String sql-exprs)))

(defn show
  ([^Dataset dataset]
   (.show dataset))
  ([^Dataset dataset n]
   (.show dataset n)))

(defn sort
  "Returns a new Dataset sorted by the specified columns, all in
  ascending order, or expressions."
  [^Dataset dataset & column-names-or-exprs]
  {:pre [(seq column-names-or-exprs)]}
  (.sort dataset (into-array column-names-or-exprs)))

(defn sort-within-partitions
  "Returns a new Dataset with each partition sorted by the given columns or expressions."
  [^Dataset dataset & column-names-or-exprs]
  {:pre [(seq column-names-or-exprs)]}
  (.sortWithinPartitions dataset (into-array column-names-or-exprs)))

(defn take
  "Returns the first n rows in the Dataset."
  [^Dataset dataset n]
  (seq (.take dataset n)))

(defn union
  "Returns a new Dataset containing union of rows in this Dataset and another Dataset."
  [^Dataset dataset ^Dataset other]
  (.union dataset other))

(defn unpersist
  "Mark the Dataset as non-persistent, and remove all blocks for it from memory and disk."
  ([^Dataset dataset]
   (.unpersist dataset))
  ([^Dataset dataset ^Boolean blocking?]
   (.unpersist dataset blocking?)))

(defn where
  "Filters rows using the given condition."
  [^Dataset dataset ^Column condition]
  (.where dataset condition))

(defn where-expr
  "Filters rows using the given SQL expression."
  [^Dataset dataset ^String expr]
  (.where dataset expr))

(defn with-column
  "Returns a new Dataset by adding a column or replacing the existing
  column that has the same name."
  [^Dataset dataset ^String column-name ^Column col]
  (.withColumn dataset column-name col))

(defn with-column-renamed
  "Returns a new Dataset with a column renamed."
  [^Dataset dataset ^String old-name ^String new-name]
  (.withColumnRenamed dataset old-name new-name))

(defn with-watermark
  "Defines an event time watermark for this Dataset."
  [^Dataset dataset ^String event-time ^String delay-threshold]
  (.withWatermark dataset event-time delay-threshold))

(defn row->map
  ([^Row row]
   (row->map row false))
  ([^Row row ^Boolean keywordize?]
   (let [t (if keywordize? ->kebab-case-keyword identity)]
     (into {}
           (map (fn [field] [(t field) (.getAs row field)]))
           (.fieldNames (.schema row))))))

(comment

  (defn mk-schema
    [col-name]
    (types/struct-type [{:name col-name :type (types/long-type) :nullable? false}]))

  (with-open [sess (spark-session {:app-name "Test"})]
    (let [data (coll->data-frame sess (mk-schema "a") (map vector (range 50)))]
      (count data)))

  (def p "/tmp/etl.6420563544458249641/session-enriched/atomic-events")

  (def q "/tmp/etl.6420563544458249641/basic-enriched/run=0/atomic-events")

  (with-open [sess (spark-session {:app-name "Test"})]
    (let [data (read-parquet sess {:base-path p} (str p "/collector_year=2017"))]
      (input-files data)))


  (with-open [sess (spark-session {:app-name "test"})]
    (let [xs (coll->data-frame sess (mk-schema "a") (map vector (range 5)))
          ys (coll->data-frame sess (mk-schema "b") (map vector (range 5)))
          zs (cross-join xs ys)]
      (show (describe zs "a" "b"))
      ))

  (with-open [sess (spark-session {:app-name "test"})]
    (let [xs (coll->data-frame sess (mk-schema "a") (map vector (range 5)))]
      (show (describe xs "a"))))


  )
