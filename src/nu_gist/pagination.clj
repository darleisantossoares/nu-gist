(ns nu-gist.pagination
  (:require [datomic.api :as d]))

(def uri "datomic:dev://localhost:4334/pagination")

;(d/create-database uri)

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
              :db/doc "Contact Name"}])


;@(d/transact conn schema)


(def customers-id (map (fn[_] (d/squuid)) (range 100)))


(rand-nth customers-id)

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


; fill the database with 150k contacts
(dotimes [_ 150000]
  @(d/transact conn [(get-random-contact (d/squuid))]))

;; count on the database
(def db (d/db conn))

(d/q '[:find (count ?e)
       :in $
       :where [?e :contact/id]] db)
; 150000




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

