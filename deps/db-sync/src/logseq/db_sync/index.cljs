(ns logseq.db-sync.index
  (:require [clojure.string :as string]
            [logseq.db-sync.common :as common]
            [promesa.core :as p]))

(def ^:private user-upsert-cache-ttl-ms (* 60 60 1000))
(def ^:private user-upsert-cache-max 1024)
(defonce ^:private *user-upsert-cache (atom {}))

(defn- prune-user-upsert-cache! [now-ms]
  (swap! *user-upsert-cache
         (fn [cache]
           (let [cache (into {}
                             (remove (fn [[_ {:keys [cached-at]}]]
                                       (>= (- now-ms cached-at) user-upsert-cache-ttl-ms)))
                             cache)
                 count-cache (count cache)]
             (if (> count-cache user-upsert-cache-max)
               (into {}
                     (drop (- count-cache user-upsert-cache-max)
                           (sort-by (comp :cached-at val) cache)))
               cache)))))

(defn- cache-user-upsert! [user-id email email-verified username now-ms]
  (swap! *user-upsert-cache assoc user-id {:email email
                                           :email-verified email-verified
                                           :username username
                                           :cached-at now-ms})
  (prune-user-upsert-cache! now-ms))

(defn- graph-e2ee-sql->bool
  [v]
  (cond
    (nil? v) true
    (or (= 1 v) (= "1" v)) true
    (or (= 0 v) (= "0" v)) false
    :else (true? v)))

(defn- graph-e2ee-bool->sql
  [v]
  (if (false? v) 0 1))

(defn- graph-ready-for-use-sql->bool
  [v]
  (cond
    (nil? v) true
    (or (= 1 v) (= "1" v)) true
    (or (= 0 v) (= "0" v)) false
    :else (true? v)))

(defn- graph-ready-for-use-bool->sql
  [v]
  (if (false? v) 0 1))

(defn- user-is-pro-sql->bool
  [v]
  (cond
    (nil? v) false
    (or (= 1 v) (= "1" v)) true
    (or (= 0 v) (= "0" v)) false
    :else (true? v)))

(defn- user-is-pro-bool->sql
  [v]
  (if (true? v) 1 0))

(defn- parse-user-groups
  [value]
  (let [parsed (cond
                 (nil? value) nil
                 (string? value) (try (js->clj (js/JSON.parse value))
                                      (catch :default _ nil))
                 (array? value) (vec value)
                 (sequential? value) value
                 :else nil)]
    (->> parsed
         (keep (fn [item]
                 (when (string? item)
                   item)))
         vec)))

(defn- user-groups->sql-json
  [groups]
  (js/JSON.stringify (clj->js (parse-user-groups groups))))

(defn- parse-int-default
  [value default]
  (cond
    (number? value) value
    (string? value) (let [n (js/parseInt value 10)]
                      (if (js/isNaN n) default n))
    :else default))

(defn- clamp-non-negative-int
  [value]
  (let [n (parse-int-default value 0)]
    (if (neg? n) 0 n)))

(def ^:private graph-e2ee-migration-sql
  "alter table graphs add column graph_e2ee INTEGER DEFAULT 1")
(def ^:private graph-ready-for-use-migration-sql
  "alter table graphs add column graph_ready_for_use integer default 1")
(def ^:private user-expire-time-migration-sql
  "alter table users add column expire_time integer")
(def ^:private user-user-groups-migration-sql
  "alter table users add column user_groups text default '[]'")
(def ^:private user-is-pro-migration-sql
  "alter table users add column is_pro integer not null default 0")
(def ^:private user-graphs-count-migration-sql
  "alter table users add column graphs_count integer not null default 0")
(def ^:private user-storage-count-migration-sql
  "alter table users add column storage_count integer not null default 0")
(def ^:private user-updated-at-migration-sql
  "alter table users add column updated_at integer")

(defn- duplicate-column-error?
  [error column-name]
  (let [message (-> (or (ex-message error) (some-> error .-message) (str error))
                    string/lower-case)]
    (and (string/includes? message "duplicate column")
         (string/includes? message (string/lower-case column-name)))))

(defn- <ensure-graph-e2ee-column!
  [db]
  (letfn [(<run-migration! []
            (-> (common/<d1-run db graph-e2ee-migration-sql)
                (p/catch (fn [error]
                           (if (duplicate-column-error? error "graph_e2ee")
                             nil
                             (p/rejected error))))))]
    (-> (p/let [result (common/<d1-all db
                                       "select name from pragma_table_info('graphs') where name = 'graph_e2ee'")
                rows (common/get-sql-rows result)]
          (when (empty? rows)
            (<run-migration!)))
        (p/catch (fn [_]
                   (<run-migration!))))))

(defn- <ensure-graph-ready-for-use-column!
  [db]
  (letfn [(<run-migration! []
            (-> (common/<d1-run db graph-ready-for-use-migration-sql)
                (p/catch (fn [error]
                           (if (duplicate-column-error? error "graph_ready_for_use")
                             nil
                             (p/rejected error))))))]
    (-> (p/let [result (common/<d1-all db
                                       "select name from pragma_table_info('graphs') where name = 'graph_ready_for_use'")
                rows (common/get-sql-rows result)]
          (when (empty? rows)
            (<run-migration!)))
        (p/catch (fn [_]
                   (<run-migration!))))))

(defn- <ensure-user-column!
  [db column-name migration-sql]
  (letfn [(<run-migration! []
            (-> (common/<d1-run db migration-sql)
                (p/catch (fn [error]
                           (if (duplicate-column-error? error column-name)
                             nil
                             (p/rejected error))))))]
    (-> (p/let [result (common/<d1-all db
                                       (str "select name from pragma_table_info('users') where name = '" column-name "'"))
                rows (common/get-sql-rows result)]
          (when (empty? rows)
            (<run-migration!)))
        (p/catch (fn [_]
                   (<run-migration!))))))

(defn- <ensure-user-expire-time-column! [db]
  (<ensure-user-column! db "expire_time" user-expire-time-migration-sql))

(defn- <ensure-user-user-groups-column! [db]
  (<ensure-user-column! db "user_groups" user-user-groups-migration-sql))

(defn- <ensure-user-is-pro-column! [db]
  (<ensure-user-column! db "is_pro" user-is-pro-migration-sql))

(defn- <ensure-user-graphs-count-column! [db]
  (<ensure-user-column! db "graphs_count" user-graphs-count-migration-sql))

(defn- <ensure-user-storage-count-column! [db]
  (<ensure-user-column! db "storage_count" user-storage-count-migration-sql))

(defn- <ensure-user-updated-at-column! [db]
  (<ensure-user-column! db "updated_at" user-updated-at-migration-sql))

(defn <index-init! [db]
  (p/do!
   (common/<d1-run db
                   (str "create table if not exists graphs ("
                        "graph_id TEXT primary key,"
                        "graph_name TEXT,"
                        "user_id TEXT,"
                        "schema_version TEXT,"
                        "graph_e2ee INTEGER DEFAULT 1,"
                        "graph_ready_for_use INTEGER DEFAULT 1,"
                        "created_at INTEGER,"
                        "updated_at INTEGER"
                        ");"))
   (<ensure-graph-e2ee-column! db)
   (<ensure-graph-ready-for-use-column! db)
   (common/<d1-run db
                   (str "create table if not exists users ("
                        "id TEXT primary key,"
                        "email TEXT,"
                        "email_verified INTEGER,"
                        "username TEXT,"
                        "expire_time INTEGER,"
                        "user_groups TEXT DEFAULT '[]',"
                        "is_pro INTEGER NOT NULL DEFAULT 0,"
                        "graphs_count INTEGER NOT NULL DEFAULT 0,"
                        "storage_count INTEGER NOT NULL DEFAULT 0,"
                        "updated_at INTEGER"
                        ");"))
   (<ensure-user-expire-time-column! db)
   (<ensure-user-user-groups-column! db)
   (<ensure-user-is-pro-column! db)
   (<ensure-user-graphs-count-column! db)
   (<ensure-user-storage-count-column! db)
   (<ensure-user-updated-at-column! db)
   (common/<d1-run db
                   (str "create table if not exists user_rsa_keys ("
                        "user_id TEXT primary key,"
                        "public_key TEXT,"
                        "encrypted_private_key TEXT,"
                        "created_at INTEGER,"
                        "updated_at INTEGER"
                        ");"))
   (common/<d1-run db
                   (str "create table if not exists graph_members ("
                        "user_id TEXT,"
                        "graph_id TEXT,"
                        "role TEXT,"
                        "invited_by TEXT,"
                        "created_at INTEGER,"
                        "primary key (user_id, graph_id),"
                        "check (role in ('manager', 'member'))"
                        ");"))
   (common/<d1-run db
                   (str "create table if not exists graph_aes_keys ("
                        "graph_id TEXT,"
                        "user_id TEXT,"
                        "encrypted_aes_key TEXT,"
                        "created_at INTEGER,"
                        "updated_at INTEGER,"
                        "primary key (graph_id, user_id)"
                        ");"))
   (common/<d1-run db
                   "create index if not exists idx_graph_members_graph_id_created_at on graph_members (graph_id, created_at)")
   (common/<d1-run db
                   "create index if not exists idx_graphs_user_id_updated_at on graphs (user_id, updated_at desc)")
   (common/<d1-run db
                   "create index if not exists idx_users_email on users (email)")))

(defn <index-list [db user-id]
  (if (string? user-id)
    (p/let [result (common/<d1-all db
                                   (str "select g.graph_id, g.graph_name, g.schema_version, g.graph_e2ee, g.graph_ready_for_use, g.created_at, g.updated_at, "
                                        "m.role, m.invited_by "
                                        "from graphs g "
                                        "left join graph_members m on g.graph_id = m.graph_id and m.user_id = ? "
                                        "where g.user_id = ? or m.user_id = ? "
                                        "order by g.updated_at desc")
                                   user-id
                                   user-id
                                   user-id)
            rows (common/get-sql-rows result)]
      (mapv (fn [row]
              {:graph-id (aget row "graph_id")
               :graph-name (aget row "graph_name")
               :schema-version (aget row "schema_version")
               :graph-e2ee? (graph-e2ee-sql->bool (aget row "graph_e2ee"))
               :graph-ready-for-use? (graph-ready-for-use-sql->bool (aget row "graph_ready_for_use"))
               :role (aget row "role")
               :invited-by (aget row "invited_by")
               :created-at (aget row "created_at")
               :updated-at (aget row "updated_at")})
            rows))
    []))

(defn <index-upsert!
  ([db graph-id graph-name user-id schema-version graph-e2ee?]
   (<index-upsert! db graph-id graph-name user-id schema-version graph-e2ee? true))
  ([db graph-id graph-name user-id schema-version graph-e2ee? graph-ready-for-use?]
   (p/let [now (common/now-ms)
           graph-e2ee? (graph-e2ee-bool->sql graph-e2ee?)
           graph-ready-for-use? (graph-ready-for-use-bool->sql graph-ready-for-use?)
           result (common/<d1-run db
                                  (str "insert into graphs (graph_id, graph_name, user_id, schema_version, graph_e2ee, graph_ready_for_use, created_at, updated_at) "
                                       "values (?, ?, ?, ?, ?, ?, ?, ?) "
                                       "on conflict(graph_id) do update set "
                                       "graph_name = excluded.graph_name, "
                                       "user_id = excluded.user_id, "
                                       "schema_version = excluded.schema_version, "
                                       "graph_e2ee = excluded.graph_e2ee, "
                                       "graph_ready_for_use = excluded.graph_ready_for_use, "
                                       "updated_at = excluded.updated_at")
                                  graph-id
                                  graph-name
                                  user-id
                                  schema-version
                                  graph-e2ee?
                                  graph-ready-for-use?
                                  now
                                  now)]
     result)))

(defn <graph-ready-for-use?
  [db graph-id]
  (when (string? graph-id)
    (p/let [result (common/<d1-all db
                                   "select graph_ready_for_use from graphs where graph_id = ?"
                                   graph-id)
            rows (common/get-sql-rows result)
            row (first rows)]
      (graph-ready-for-use-sql->bool (some-> row (aget "graph_ready_for_use"))))))

(defn <graph-ready-for-use-set!
  [db graph-id graph-ready-for-use?]
  (when (string? graph-id)
    (common/<d1-run db
                    "update graphs set graph_ready_for_use = ?, updated_at = ? where graph_id = ?"
                    (graph-ready-for-use-bool->sql graph-ready-for-use?)
                    (common/now-ms)
                    graph-id)))

(defn <graph-delete-metadata! [db graph-id]
  (p/do!
   (common/<d1-run db "delete from graph_aes_keys where graph_id = ?" graph-id)
   (common/<d1-run db "delete from graph_members where graph_id = ?" graph-id)))

(defn <graph-delete-index-entry! [db graph-id]
  (common/<d1-run db "delete from graphs where graph_id = ?" graph-id))

(defn <graph-name-exists?
  [db graph-name user-id]
  (when (and (string? graph-name) (string? user-id))
    (p/let [result (common/<d1-all db
                                   "select graph_id from graphs where graph_name = ? and user_id = ?"
                                   graph-name
                                   user-id)
            rows (common/get-sql-rows result)]
      (boolean (seq rows)))))

(defn <user-upsert! [db claims]
  (let [user-id (aget claims "sub")]
    (when (string? user-id)
      (let [email (aget claims "email")
            email-verified (aget claims "email_verified")
            username (aget claims "cognito:username")
            email-verified (cond
                             (true? email-verified) 1
                             (false? email-verified) 0
                             :else nil)
            now (common/now-ms)
            cached (get @*user-upsert-cache user-id)]
        (if (and cached
                 (= email (:email cached))
                 (= email-verified (:email-verified cached))
                 (= username (:username cached))
                 (< (- now (:cached-at cached)) user-upsert-cache-ttl-ms))
          (cache-user-upsert! user-id email email-verified username now)
          (p/let [result (common/<d1-run db
                                         (str "insert into users (id, email, email_verified, username) "
                                              "values (?, ?, ?, ?) "
                                              "on conflict(id) do update set "
                                              "email = excluded.email, "
                                              "email_verified = excluded.email_verified, "
                                              "username = excluded.username")
                                         user-id
                                         email
                                         email-verified
                                         username)]
            (cache-user-upsert! user-id email email-verified username now)
            result))))))

(defn <user-id-by-email [db email]
  (when (string? email)
    (p/let [result (common/<d1-all db {:session "first-primary"}
                                   "select id from users where email = ?"
                                   email)
            rows (common/get-sql-rows result)
            row (first rows)]
      (when row
        (aget row "id")))))

(defn <user-sync-info
  [db user-id]
  (when (string? user-id)
    (p/let [result (common/<d1-all db {:session "first-primary"}
                                   "select expire_time, user_groups, is_pro, graphs_count, storage_count, updated_at from users where id = ?"
                                   user-id)
            rows (common/get-sql-rows result)
            row (first rows)]
      {:expire-time (parse-int-default (some-> row (aget "expire_time")) 0)
       :user-groups (parse-user-groups (some-> row (aget "user_groups")))
       :is-pro (user-is-pro-sql->bool (some-> row (aget "is_pro")))
       :graphs-count (clamp-non-negative-int (some-> row (aget "graphs_count")))
       :storage-count (clamp-non-negative-int (some-> row (aget "storage_count")))
       :updated-at (parse-int-default (some-> row (aget "updated_at")) 0)})))

(defn <users-with-email
  [db]
  (p/let [result (common/<d1-all db {:session "first-primary"}
                                 "select id, email, user_groups, is_pro, expire_time, graphs_count, storage_count from users where email is not null and email != ''")
          rows (common/get-sql-rows result)]
    (mapv (fn [row]
            {:user-id (aget row "id")
             :email (some-> (aget row "email") string/lower-case)
             :user-groups (parse-user-groups (aget row "user_groups"))
             :is-pro (user-is-pro-sql->bool (aget row "is_pro"))
             :expire-time (parse-int-default (aget row "expire_time") 0)
             :graphs-count (clamp-non-negative-int (aget row "graphs_count"))
             :storage-count (clamp-non-negative-int (aget row "storage_count"))})
          rows)))

(defn <user-sync-update!
  [db user-id {:keys [expire-time user-groups is-pro graphs-count storage-count updated-at]}]
  (when (string? user-id)
    (common/<d1-run db
                    (str "update users set expire_time = ?, user_groups = ?, is_pro = ?, graphs_count = ?, storage_count = ?, updated_at = ? "
                         "where id = ?")
                    (parse-int-default expire-time 0)
                    (user-groups->sql-json user-groups)
                    (user-is-pro-bool->sql is-pro)
                    (clamp-non-negative-int graphs-count)
                    (clamp-non-negative-int storage-count)
                    (parse-int-default updated-at (common/now-ms))
                    user-id)))

(defn <user-sync-update-by-email!
  [db email fields]
  (when (string? email)
    (p/let [user-id (<user-id-by-email db (string/lower-case email))]
      (when (string? user-id)
        (<user-sync-update! db user-id fields)))))

(defn <user-rsa-key-pair-upsert!
  [db user-id public-key encrypted-private-key]
  (when (string? user-id)
    (let [now (common/now-ms)]
      (common/<d1-run db
                      (str "insert into user_rsa_keys (user_id, public_key, encrypted_private_key, created_at, updated_at) "
                           "values (?, ?, ?, ?, ?) "
                           "on conflict(user_id) do update set "
                           "public_key = excluded.public_key, "
                           "encrypted_private_key = excluded.encrypted_private_key, "
                           "updated_at = excluded.updated_at")
                      user-id
                      public-key
                      encrypted-private-key
                      now
                      now))))

(defn <user-rsa-key-pair
  [db user-id]
  (when (string? user-id)
    (p/let [result (common/<d1-all db {:session "first-primary"}
                                   "select public_key, encrypted_private_key from user_rsa_keys where user_id = ?"
                                   user-id)
            rows (common/get-sql-rows result)
            row (first rows)]
      (when row
        {:public-key (aget row "public_key")
         :encrypted-private-key (aget row "encrypted_private_key")}))))

(defn <user-rsa-public-key-by-email
  [db email]
  (when (string? email)
    (p/let [result (common/<d1-all db {:session "first-primary"}
                                   (str "select k.public_key from user_rsa_keys k "
                                        "left join users u on k.user_id = u.id "
                                        "where u.email = ?")
                                   email)
            rows (common/get-sql-rows result)
            row (first rows)]
      (when row
        (aget row "public_key")))))

(defn <graph-encrypted-aes-key-upsert!
  [db graph-id user-id encrypted-aes-key]
  (when (and (string? graph-id) (string? user-id))
    (let [now (common/now-ms)]
      (common/<d1-run db
                      (str "insert into graph_aes_keys (graph_id, user_id, encrypted_aes_key, created_at, updated_at) "
                           "values (?, ?, ?, ?, ?) "
                           "on conflict(graph_id, user_id) do update set "
                           "encrypted_aes_key = excluded.encrypted_aes_key, "
                           "updated_at = excluded.updated_at")
                      graph-id
                      user-id
                      encrypted-aes-key
                      now
                      now))))

(defn <graph-encrypted-aes-key
  [db graph-id user-id]
  (when (and (string? graph-id) (string? user-id))
    (p/let [result (common/<d1-all db
                                   "select encrypted_aes_key from graph_aes_keys where graph_id = ? and user_id = ?"
                                   graph-id
                                   user-id)
            rows (common/get-sql-rows result)
            row (first rows)]
      (when row
        (aget row "encrypted_aes_key")))))

(defn <graph-member-upsert! [db graph-id user-id role invited-by]
  (let [now (common/now-ms)]
    (common/<d1-run db
                    (str "insert into graph_members (user_id, graph_id, role, invited_by, created_at) "
                         "values (?, ?, ?, ?, ?) "
                         "on conflict(user_id, graph_id) do update set "
                         "role = excluded.role, "
                         "invited_by = excluded.invited_by")
                    user-id
                    graph-id
                    role
                    invited-by
                    now)))

(defn <graph-members-list [db graph-id]
  (p/let [result (common/<d1-all db {:session "first-primary"}
                                 (str "select m.user_id, m.graph_id, m.role, m.invited_by, m.created_at, "
                                      "u.email, u.username "
                                      "from graph_members m "
                                      "left join users u on m.user_id = u.id "
                                      "where m.graph_id = ? order by m.created_at asc")
                                 graph-id)
          rows (common/get-sql-rows result)]
    (mapv (fn [row]
            {:user-id (aget row "user_id")
             :graph-id (aget row "graph_id")
             :role (aget row "role")
             :invited-by (aget row "invited_by")
             :created-at (aget row "created_at")
             :email (aget row "email")
             :username (aget row "username")})
          rows)))

(defn <graph-member-update-role! [db graph-id user-id role]
  (common/<d1-run db
                  (str "update graph_members set role = ? "
                       "where graph_id = ? and user_id = ?")
                  role
                  graph-id
                  user-id))

(defn <graph-member-delete! [db graph-id user-id]
  (common/<d1-run db
                  "delete from graph_members where graph_id = ? and user_id = ?"
                  graph-id
                  user-id))

(defn <graph-member-role [db graph-id user-id]
  (when (and (string? graph-id) (string? user-id))
    (p/let [result (common/<d1-all db
                                   "select role from graph_members where graph_id = ? and user_id = ?"
                                   graph-id
                                   user-id)
            rows (common/get-sql-rows result)
            row (first rows)]
      (when row
        (aget row "role")))))

(defn <user-has-access-to-graph? [db graph-id user-id]
  (when (and (string? graph-id) (string? user-id))
    (p/let [result (common/<d1-all db
                                   (str "select graph_id from graphs where graph_id = ? and user_id = ? "
                                        "union select graph_id from graph_members where graph_id = ? and user_id = ?")
                                   graph-id
                                   user-id
                                   graph-id
                                   user-id)
            rows (common/get-sql-rows result)]
      (boolean (seq rows)))))

(defn <user-is-manager? [db graph-id user-id]
  (when (and (string? graph-id) (string? user-id))
    (p/let [result (common/<d1-all db
                                   (str "select graph_id from graphs where graph_id = ? and user_id = ? "
                                        "union select graph_id from graph_members where graph_id = ? and user_id = ? and role = 'manager'")
                                   graph-id
                                   user-id
                                   graph-id
                                   user-id)
            rows (common/get-sql-rows result)]
      (boolean (seq rows)))))
