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
              :db/tupleAttrs [:contact/customer-id :contact/name :contact/id]
              :db/index true
              :db/doc "Tuple created to maintain the desired order"}])


@(d/transact conn schema)


(def customers-id (map (fn[_] (d/squuid)) (range 100)))

(def names ["Joe" "Stu" "Chris" "Dan" "Kyle"
            "Mateus" "Filipe" "Monteiro"
            "Robert" "Jordan" "Rich" "Mello"
            "Darlei" "Hanna" "Carol" "Fernanda"
            "Robson" "Keith" "David" "Justin" "Jeb"
            "Jaret" "Gabi" "Alex" "Michael"
            "Jarrod" "Joao" "Georgia" "Jennifer"
            "Marcela" "Andre" "Christian" "Guilherme"
            "Lucas" "Bruna" "Rafael" "Pedro" "Mariana"
            "Henrique" "Sofia" "Gabriel" "Beatriz"
            "Nicolas" "Arthur" "Livia" "Thiago" "Lara"
            "Camila" "Leonardo" "Vitor" "Miguel" "Julia"
            "Alexandre" "Jean" "Luc" "Pierre" "Marie"
            "Claire" "Sophie" "Julien" "Thomas" "Nathalie"
            "Jacques" "Isabelle" "Chloe" "Maxime" "Elodie"
            "Antoine" "Camille" "Damien" "Manon" "Florian"
            "Hugo" "Emma" "Lucas" "Lea" "Louis" "Amandine"
            "Noah" "Elise" "Dylan" "Juliette" "Charles"
            "Alain" "Marc" "Brigitte" "Laurent" "Charlotte"
            "Nicolas" "Celine" "Yves" "Sandrine" "Stephane"
            "David" "Alice" "Valerie" "Pascal" "Francois"
            "Catherine" "Paul" "Gabrielle" "Jean-Paul"
            "Anna" "Mark" "James" "William" "John"
            "Mary" "Patricia" "Jennifer" "Linda" "Elizabeth"
            "Barbara" "Susan" "Jessica" "Sarah" "Karen"
            "Nancy" "Lisa" "Betty" "Margaret" "Sandra"
            "Ashley" "Kimberly" "Emily" "Donna" "Michelle"
            "Carol" "Amanda" "Dorothy" "Melissa" "Deborah"
            "Stephanie" "Rebecca" "Sharon" "Laura" "Cynthia"
            "Kathleen" "Amy" "Shirley" "Angela" "Helen"
            "Anna" "Brenda" "Pamela" "Nicole" "Samantha"
            "Katherine" "Emma" "Ruth" "Christine" "Catherine"
            "Debra" "Rachel" "Carolyn" "Janet" "Virginia"
            "Maria" "Heather" "Diane" "Julie" "Joyce"
            "Victoria" "Kelly" "Christina" "Joan" "Evelyn"
            "Lauren" "Judith" "Olivia" "Frances" "Martha"
            "Cheryl" "Megan" "Andrea" "Hannah" "Jacqueline"
            "Ann" "Jean" "Alice" "Kathryn" "Gloria"
            "Teresa" "Doris" "Sara" "Janice" "Julia"
            "Marie" "Madison" "Grace" "Judy" "Theresa"
            "Beverly" "Denise" "Marilyn" "Amber" "Danielle"
            "Rose" "Brittany" "Diana" "Abigail" "Natalie"
            "Jane" "Lori" "Tiffany" "Alexis" "Kayla"
            "Haruto" "Yuto" "Sota" "Yuma" "Ren"
            "Hina" "Aoi" "Yui" "Sakura" "Hana"
            "Haruki" "Kaito" "Koki" "Hinata" "Asahi"
            "Miyu" "Akari" "Rin" "Ayaka" "Sara"])


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



