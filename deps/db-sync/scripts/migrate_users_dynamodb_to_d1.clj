#!/usr/bin/env bb

(ns migrate-users-dynamodb-to-d1
  (:require [babashka.process :as process]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def allowed-envs #{"local" "staging" "prod"})

(def default-options
  {:env "prod"
   :aws-table "user-info"
   :aws-region "us-east-1"
   :aws-profile "prod"
   :database "DB"
   :config "worker/wrangler.toml"
   :chunk-size 200
   :dry-run false
   :max-users nil})

(defn fail!
  [message]
  (binding [*out* *err*]
    (println message))
  (System/exit 1))

(defn usage
  []
  (str
   "Migrate legacy AWS DynamoDB users into db-sync D1 users table.\n\n"
   "Usage:\n"
   "  bb migrate:users --env <local|staging|prod> [options]\n\n"
   "Options:\n"
   "  --env <env>             Target D1 env: local | staging | prod (default: prod).\n"
   "  --aws-table <name>      DynamoDB table name (default: user-info).\n"
   "  --aws-region <region>   AWS region for DynamoDB scan (default: us-east-1).\n"
   "  --aws-profile <name>    AWS profile name (default: prod).\n"
   "  --database <name>       D1 database binding/name for wrangler (default: DB).\n"
   "  --config <path>         Wrangler config path (default: worker/wrangler.toml).\n"
   "  --chunk-size <n>        Users per D1 execute batch (default: 200).\n"
   "  --max-users <n>         Stop after migrating N users (for smoke tests).\n"
   "  --dry-run               Scan and print stats without writing D1.\n"
   "  -h, --help              Show this help.\n\n"
   "Source mapping (from backend-lambda user-info):\n"
   "  user-uuid -> users.id\n"
   "  email -> users.email\n"
   "  file-sync-service-expire-time -> users.expire_time\n"
   "  groups -> users.user_groups\n"
   "  is_pro inferred as expire_time > now (unless explicit is-pro exists).\n"))

(def option->key
  {"--env" :env
   "--aws-table" :aws-table
   "--aws-region" :aws-region
   "--aws-profile" :aws-profile
   "--database" :database
   "--config" :config
   "--chunk-size" :chunk-size
   "--max-users" :max-users})

(defn parse-int!
  [label value]
  (let [v (parse-long (str value))]
    (when-not (some? v)
      (fail! (str "Invalid integer for " label ": " value)))
    v))

(defn parse-args
  [argv]
  (loop [opts default-options
         args argv]
    (if (empty? args)
      opts
      (let [token (first args)]
        (cond
          (contains? #{"-h" "--help"} token)
          (assoc opts :help true)

          (= token "--dry-run")
          (recur (assoc opts :dry-run true) (rest args))

          (str/starts-with? token "--")
          (let [[flag inline-value] (if (str/includes? token "=")
                                      (str/split token #"=" 2)
                                      [token nil])
                option-key (get option->key flag)]
            (when-not option-key
              (fail! (str "Unknown option: " flag)))
            (let [remaining (rest args)
                  value (or inline-value (first remaining))]
              (when (and (nil? inline-value) (empty? remaining))
                (fail! (str "Missing value for option: " flag)))
              (recur (assoc opts option-key value)
                     (if inline-value
                       (rest args)
                       (rest remaining)))))

          :else
          (fail! (str "Unexpected argument: " token)))))))

(defn normalize-options
  [opts]
  (if (:help opts)
    opts
    (let [env (:env opts)
          chunk-size (parse-int! "--chunk-size" (:chunk-size opts))
          max-users (some-> (:max-users opts) (parse-int! "--max-users"))
          config-path (.getAbsolutePath (io/file (:config opts)))]
      (when-not (contains? allowed-envs env)
        (fail! (str "Invalid --env: " env ". Expected one of: " (str/join ", " (sort allowed-envs)))))
      (when (<= chunk-size 0)
        (fail! "--chunk-size must be > 0"))
      (when (and max-users (<= max-users 0))
        (fail! "--max-users must be > 0"))
      (-> opts
          (assoc :chunk-size chunk-size)
          (assoc :max-users max-users)
          (assoc :config config-path)))))

(defn run-cmd!
  [cmd]
  (let [{:keys [exit out err]} @(process/process cmd {:out :string :err :string})]
    (when-not (zero? exit)
      (throw (ex-info "Command failed"
                      {:cmd cmd
                       :exit exit
                       :out out
                       :err err})))
    out))

(defn run-json-cmd!
  [cmd]
  (json/parse-string (run-cmd! cmd)))

(defn aws-scan-page!
  [{:keys [aws-table aws-region aws-profile]} exclusive-start-key]
  (let [base-cmd ["aws" "dynamodb" "scan"
                  "--table-name" aws-table
                  "--region" aws-region
                  "--output" "json"]
        with-profile (if (str/blank? aws-profile)
                       base-cmd
                       (concat base-cmd ["--profile" aws-profile]))
        cmd (if (some? exclusive-start-key)
              (concat with-profile ["--exclusive-start-key" (json/generate-string exclusive-start-key)])
              with-profile)]
    (run-json-cmd! (vec cmd))))

(defn parse-dynamo-number
  [s]
  (or (parse-long (str s))
      (try
        (Double/parseDouble (str s))
        (catch Exception _ nil))))

(defn decode-dynamo-attr
  [attr]
  (cond
    (contains? attr "S") (get attr "S")
    (contains? attr "N") (parse-dynamo-number (get attr "N"))
    (contains? attr "BOOL") (boolean (get attr "BOOL"))
    (contains? attr "NULL") nil
    (contains? attr "SS") (vec (get attr "SS"))
    (contains? attr "NS") (mapv parse-dynamo-number (get attr "NS"))
    (contains? attr "L") (mapv decode-dynamo-attr (get attr "L"))
    (contains? attr "M") (into {} (map (fn [[k v]] [k (decode-dynamo-attr v)]) (get attr "M")))
    :else nil))

(defn decode-dynamo-item
  [item]
  (into {}
        (map (fn [[k v]] [k (decode-dynamo-attr v)]) item)))

(defn normalize-groups
  [groups]
  (->> (cond
         (nil? groups) []
         (sequential? groups) groups
         (set? groups) groups
         :else [groups])
       (keep (fn [group]
               (when (string? group)
                 (let [s (str/trim group)]
                   (when-not (str/blank? s)
                     s)))))
       distinct
       vec))

(defn to-non-negative-int
  [value]
  (let [n (cond
            (integer? value) value
            (number? value) (long value)
            (string? value) (or (parse-long value) 0)
            :else 0)]
    (max 0 n)))

(defn dynamo-item->d1-user
  [item now-seconds now-ms]
  (let [user-id (some-> (get item "user-uuid") str str/trim not-empty)]
    (when user-id
      (let [expire-time (to-non-negative-int (get item "file-sync-service-expire-time" 0))
            explicit-is-pro (get item "is-pro")
            is-pro (cond
                     (boolean? explicit-is-pro) explicit-is-pro
                     (number? explicit-is-pro) (pos? (long explicit-is-pro))
                     (string? explicit-is-pro)
                     (contains? #{"1" "true" "TRUE" "yes" "YES"} explicit-is-pro)
                     :else (> expire-time now-seconds))
            email (some-> (get item "email") str str/lower-case str/trim not-empty)
            user-groups (normalize-groups (get item "groups"))
            graphs-count (to-non-negative-int (get item "graphs-count"))
            storage-count (to-non-negative-int (get item "storage-count"))]
        {:id user-id
         :email email
         :expire-time expire-time
         :user-groups user-groups
         :is-pro is-pro
         :graphs-count graphs-count
         :storage-count storage-count
         :updated-at now-ms}))))

(defn sql-quote
  [value]
  (str "'" (str/replace (str value) "'" "''") "'"))

(defn sql-text
  [value]
  (if (nil? value)
    "NULL"
    (sql-quote value)))

(defn user->upsert-sql
  [{:keys [id email expire-time user-groups is-pro graphs-count storage-count updated-at]}]
  (let [groups-json (json/generate-string user-groups)
        is-pro-sql (if is-pro 1 0)]
    (str "insert into users (id, email, email_verified, username, expire_time, user_groups, is_pro, graphs_count, storage_count, updated_at) "
         "values ("
         (sql-text id) ", "
         (sql-text email) ", "
         "NULL, NULL, "
         expire-time ", "
         (sql-text groups-json) ", "
         is-pro-sql ", "
         graphs-count ", "
         storage-count ", "
         updated-at
         ") "
         "on conflict(id) do update set "
         "email = coalesce(excluded.email, users.email), "
         "email_verified = coalesce(excluded.email_verified, users.email_verified), "
         "username = coalesce(excluded.username, users.username), "
         "expire_time = excluded.expire_time, "
         "user_groups = excluded.user_groups, "
         "is_pro = excluded.is_pro, "
         "graphs_count = case when excluded.graphs_count > 0 then excluded.graphs_count else users.graphs_count end, "
         "storage_count = case when excluded.storage_count > 0 then excluded.storage_count else users.storage_count end, "
         "updated_at = excluded.updated_at;")))

(defn build-wrangler-cmd
  [{:keys [env database config]} sql-file]
  (let [base ["npx" "--yes" "wrangler" "d1" "execute" database
              "--config" config
              "--json"
              "--file" sql-file]]
    (if (= env "local")
      (conj base "--local")
      (vec (concat base ["--env" env "--remote"])))))

(defn wrangler-success?
  [result]
  (cond
    (vector? result) (every? #(true? (get % "success")) result)
    (map? result) (true? (get result "success"))
    :else false))

(defn execute-upsert-chunk!
  [opts users]
  (let [sql-lines (map user->upsert-sql users)
        sql (str "begin;\n"
                 (str/join "\n" sql-lines)
                 "\ncommit;\n")
        sql-file (doto (java.io.File/createTempFile "db-sync-users-migrate-" ".sql")
                   (.deleteOnExit))]
    (spit sql-file sql)
    (let [result (run-json-cmd! (build-wrangler-cmd opts (.getAbsolutePath sql-file)))]
      (when-not (wrangler-success? result)
        (throw (ex-info "Wrangler D1 execute failed"
                        {:result result
                         :chunk-size (count users)})))
      (count users))))

(defn migrate-page-users!
  [opts users]
  (let [chunk-size (:chunk-size opts)
        dry-run (:dry-run opts)]
    (reduce (fn [acc chunk]
              (if dry-run
                (+ acc (count chunk))
                (+ acc (execute-upsert-chunk! opts chunk))))
            0
            (partition-all chunk-size users))))

(defn run-migration!
  [opts]
  (let [now-ms (System/currentTimeMillis)
        now-seconds (quot now-ms 1000)
        max-users (:max-users opts)]
    (loop [exclusive-start-key nil
           scanned-items 0
           valid-users 0
           migrated-users 0
           skipped-items 0
           page-index 0]
      (let [page (aws-scan-page! opts exclusive-start-key)
            raw-items (vec (get page "Items" []))
            decoded-items (mapv decode-dynamo-item raw-items)
            all-users (vec (keep #(dynamo-item->d1-user % now-seconds now-ms) decoded-items))
            remaining (when max-users (- max-users valid-users))
            selected-users (if (and remaining (<= remaining 0))
                             []
                             (vec (if remaining (take remaining all-users) all-users)))
            migrated-this-page (migrate-page-users! opts selected-users)
            scanned-items* (+ scanned-items (count raw-items))
            valid-users* (+ valid-users (count selected-users))
            migrated-users* (+ migrated-users migrated-this-page)
            skipped-items* (+ skipped-items (- (count raw-items) (count all-users)))
            hit-max? (and max-users (>= valid-users* max-users))
            last-evaluated-key (get page "LastEvaluatedKey")]
        (println (format "[page %d] scanned=%d selected=%d migrated=%d skipped=%d"
                         (inc page-index)
                         (count raw-items)
                         (count selected-users)
                         migrated-this-page
                         (- (count raw-items) (count all-users))))
        (if (or hit-max? (nil? last-evaluated-key) (empty? last-evaluated-key))
          {:scanned-items scanned-items*
           :valid-users valid-users*
           :migrated-users migrated-users*
           :skipped-items skipped-items*}
          (recur last-evaluated-key
                 scanned-items*
                 valid-users*
                 migrated-users*
                 skipped-items*
                 (inc page-index)))))))

(defn -main
  [& argv]
  (let [opts (-> (parse-args argv)
                 normalize-options)]
    (when (:help opts)
      (println (usage))
      (System/exit 0))

    (println "Starting DynamoDB -> D1 user migration with options:")
    (println (pr-str (dissoc opts :help)))

    (let [{:keys [scanned-items valid-users migrated-users skipped-items]} (run-migration! opts)]
      (println "---")
      (println (format "Done. scanned=%d valid=%d migrated=%d skipped=%d dry-run=%s"
                       scanned-items
                       valid-users
                       migrated-users
                       skipped-items
                       (if (:dry-run opts) "true" "false"))))))

(when (= *file* (System/getProperty "babashka.file"))
  (try
    (apply -main *command-line-args*)
    (catch Exception e
      (binding [*out* *err*]
        (println "Migration failed:" (.getMessage e))
        (when-let [data (ex-data e)]
          (when-let [cmd (:cmd data)]
            (println "Command:" (str/join " " cmd)))
          (when-let [err (:err data)]
            (when-not (str/blank? err)
              (println "stderr:" err)))
          (when-let [out (:out data)]
            (when-not (str/blank? out)
              (println "stdout:" out)))))
      (System/exit 1))))
