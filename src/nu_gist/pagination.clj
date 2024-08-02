(ns nu-gist.pagination
  (:require [datomic.api :as d]))

(def uri "datomic:dev://localhost:4334/pagination")

(d/create-database uri)

;(d/delete-database uri)

(def conn (d/connect uri))


(def schema [{:db/ident :contact/id
              :db/cardinality :db.cardinality/one
              :db/valueType :db.type/uuid
              :db/doc "Contact ID"
              :db/unique :db.unique/identity}
             {:db/ident :contact/customer-id
              :db/cardinality :db.cardinality/one
              :db/valueType :db.type/uuid
              :db/doc "Contact Id which owns the contact"
              :db/index true}
             {:db/ident :contact/name
              :db/cardinality :db.cardinality/one
              :db/valueType :db.type/string
              :db/doc "Contact Name Indexed because this is the sort criteria"}
             {:db/ident :contact/customer-id+contact-name+contact-id
              :db/cardinality :db.cardinality/one
              :db/valueType :db.type/tuple
              :db/tupleAttrs [:contact/customer-id :contact/name :contact/id] ;contact/id will be used as a tie breaker
              :db/index true}])


@(d/transact conn schema)


(def customers-id (map (fn[_] (d/squuid)) (range 100)))

(def names ["Joe" "Stu" "Chris" "Dan" "Kyle"
            "Mateus" "Filipe" "Monteiro"
            "Robert" "Jordan" "Rich" "Mello"
            "Darlei" "Hanna" "Carol" "Fernanda"
            "Robson" "Keith" "David" "Justin" "Jeb"
            "Jaret" "Gabi" "Alex" "Michael"
            "Jarrod" "Joao" "Georgia" "Jennifer"
            "Marcela" "Andre" "Christian" "Guilherme"])


(defn get-random-contact
  "Returns a contact"
  [contact-id]
  {:contact/id contact-id
   :contact/customer-id (rand-nth customers-id)
   :contact/name (rand-nth names)})



; fill the database with 150k contacts, using different dotimes to have different db Ts
(dotimes [_ 100]
  (dotimes [_ 1500]
    @(d/transact conn [(get-random-contact (d/squuid))])))

;; count on the database
(def db (d/db conn))

(d/q '[:find (count ?e)
       :in $
       :where [?e :contact/id]] db)
; 150000


;; get random customer-id
(def random-customer-id (rand-nth (d/q '[:find [?customer-id ...]
                                         :in $
                                         :where [?e :contact/customer-id ?customer-id]] db)))

random-customer-id


;; get all contacts from that customer
(count (d/q '[:find (pull ?c [:contact/customer-id+contact-name])
        :in $ ?c-id
        :where [?c :contact/customer-id ?c-id]] db random-customer-id))
;1492 in my case




(d/q '[:find (pull ?e [*])
       :in $ ?c-id
       :where [?e :contact/customer-id ?c-id]]
     (d/db conn) random-customer-id)

(def db (d/db conn))

(d/basis-t db) ; current t for our DB, we need to encrypt the t on the answer


(def db-t (d/as-of db (d/basis-t db)))

(def first-page (take 100 (d/index-pull db-t {:index :avet
                                                      :selector '[:contact/customer-id+contact-name+contact-id :contact/name :contact/id :contact/customer-id]
                                                      :start [:contact/customer-id+contact-name+contact-id [random-customer-id]]})))


(:contact/customer-id+contact-name+contact-id (last first-page))

(take 100 (d/index-pull db-t {:index :avet
                              :selector '[:contact/name]
                              :start [:contact/customer-id+contact-name+contact-id (:contact/customer-id+contact-name+contact-id (last first-page))]}))


(:io-stats (d/query {:query '[:find ?e
                    :in $ ?customer-id
                    :where [?e :contact/customer-id ?customer-id]]
           :args [db-t random-customer-id]
           :io-context :tips-and-tricks/pagination}))




;;; get history of an entity
(def story [:instrument/id #uuid "1A119507-3388-4401-890C-7C09B22DD507"])


(->> (d/q '[:find ?aname ?v ?tx ?inst ?added
            :in $ ?e
            :where
            [?e ?a ?v ?tx ?added]
            [?a :db/ident ?aname]
            [?tx :db/txInstant ?inst]]
          (d/history (d/db conn))
          story)
     seq
     (sort-by #(first % ))
     println)

