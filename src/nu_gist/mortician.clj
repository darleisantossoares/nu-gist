(ns nu-gist.mortician
  (:require [datomic.api :as d]
            [common-datomic.api :as cd]
            [common-datomic.extensions.transformations :as cd-t]
            [common-core.time]))

(def deadletter-schema
  [#:db{:ident :deadletter/id
       :valueType :db.type/uuid
       :cardinality :db.cardinality/one
       :unique :db.unique/identity}
   #:db{:ident :deadletter/status
        :valueType :db.type/ref
        :cardinality :db.cardinality/one
        :index true}
   #:db{:ident :deadletter/topic
        :valueType :db.type/string
        :cardinality :db.cardinality/one
        :index true}
   #:db{:ident :deadletter/producer
        :valueType :db.type/string
        :cardinality :db.cardinality/one
        :index true}
   #:db{:ident :deadletter/replay-at
        :valueType :db.type/instant
        :cardinality :db.cardinality/one
        :index true}
   #:db{:ident :deadletter/consumed-at
        :valueType :db.type/string
        :cardinality :db.cardinality/one
        :index true}
   #:db{:ident :deadletter/expiration-date
        :valueType :db.type/instant
        :cardinality :db.cardinality/one
        :index true}
   #:db{:ident :deadletter/cid
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident :deadletter/exception-name
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident :deadletter/s3-path
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident :deadletter/errors
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
    #:db{:ident :deadletter/payload-hash
        :valueType :db.type/string
        :cardinality :db.cardinality/one
        :index true}
   #:db{:ident :deadletter/service-owner
        :valueType :db.type/string
        :cardinality :db.cardinality/one
        :index true}
   #:db{:ident :deadletter/partition-key
        :valueType :db.type/string
        :cardinality :db.cardinality/one
        :index true}
   #:db{:ident :deadletter/status+topic
        :valueType :db.type/tuple
        :cardinality :db.cardinality/one
        :index true
        :tupleAttrs [:deadletter/status :deadletter/topic]}
   #:db{:ident :deadletter/status+producer
        :valueType :db.type/tuple
        :cardinality :db.cardinality/one
        :index true
        :tupleAttrs [:deadletter/status :deadletter/producer]}
   #:db{:ident :deadletter/status+service-owner
        :valueType :db.type/tuple
        :cardinality :db.cardinality/one
        :index true
        :tupleAttrs [:deadletter/status :deadletter/service-owner]}
   #:db{:ident :deadletter.status/to-process}
   #:db{:ident :deadletter.status/replayed}
   #:db{:ident :deadletter.status/dropped}
   #:db{:ident :deadletter.status/stucked}
   #:db{:ident :deadletter.status/to-replay}])

(def uri "datomic:dev://localhost:4334/mortician")
;(d/create-database uri)
(def conn (d/connect uri))


@(d/transact conn deadletter-schema)



(def status-stucked (d/entid (d/db conn) :deadletter.status/stucked))
(def status-to-process (d/entid (d/db conn) :deadletter.status/to-process))
(def status-replay (d/entid (d/db conn) :deadletter.status/to-replay))

status-stucked
status-to-process
status-replay


(defn list-next-to-replay
  [date
   db]
  (cd/entities '{:find  [?e]
                 :in    [$ ?date]
                 :where [[?e :deadletter/status :deadletter.status/to-replay]
                         [?e :deadletter/replay-at ?replay-at]
                         [(common-core.time/before-or-equal? ?replay-at ?date)]
                         ]}
               db, date))

(def db (d/db conn))

(list-next-to-replay (java.util.Date.) db)


(def topics ["TRANSFER.IN" "TRANSFER.OUT" "CREATE-CONTACT" "CC"])
(def producers ["Timbaland" "Adele" "Sr Barriga" "Contacts-Manager"])


(defn generate-deadletters
  [n]
  (vec (repeatedly n (fn []
                       {:deadletter/id (d/squuid)
                        :deadletter/status status-replay
                        :deadletter/topic (rand-nth topics)
                        :deadletter/producer (rand-nth producers)
                        :deadletter/replay-at (java.util.Date.)
                        :deadletter/cid (str "cid." (rand-int 10000))}))))

;@(d/transact conn (generate-deadletters 10000))

;; lazily reading from the database
(:ret (datomic.measure.io-stats/with-io-stats
        #(cd-t/transform-map db (take 5000 (d/qseq {:query '[:find ?e
                                       :in $ ?status-replay
                                       :where [?e :deadletter/status ?status-replay]]
                              :args [db status-replay]})))
        {:io-context :test/foo}))


(take 5000 (d/qseq {:query '[:find [?e ...]
                               :in $ ?status-replay
                               :where [?e :deadletter/status ?status-replay]]
                      :args [db status-replay]}))



(mapv (fn [eid] (d/entid db eid)) (take 5000 (d/qseq {:query '[:find [?e ...]
                                                    :in $ ?status-replay
                                                    :where [?e :deadletter/status ?status-replay]]
                                           :args [db status-replay]})))


(mapv (fn [eid] (cd-t/transform-map db (d/entity db eid))) (take 5000 (d/qseq {:query '[:find [?e ...]
                                                                 :in $ ?status-replay
                                                                 :where [?e :deadletter/status ?status-replay]]
                                                        :args [db status-replay]})))



(:io-stats
 (datomic.measure.io-stats/with-io-stats
   #(mapv (fn [eid] (cd-t/transform-map db (d/entity db eid))) (take 5000 (d/qseq {:query '[:find [?e ...]
                                                                                            :in $ ?status-replay
                                                                                            :where [?e :deadletter/status ?status-replay]]
                                                                                   :args [db status-replay]})))
   {:io-context :common/datomic}))

(:io-stats (datomic.measure.io-stats/with-io-stats
             #(cd/entities '{:find  [?e]
                             :in    [$ ?date]
                             :where [[?e :deadletter/status :deadletter.status/to-replay]
                                     [?e :deadletter/replay-at ?replay-at]
                                     [(common-core.time/before-or-equal? ?replay-at ?date)]]}
                           db, (java.util.Date.))
             {:io-context :common/datomic}))




