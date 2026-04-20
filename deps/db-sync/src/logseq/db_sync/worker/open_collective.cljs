(ns logseq.db-sync.worker.open-collective
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [lambdaisland.glogi :as log]
            [logseq.db-sync.common :as common]
            [logseq.db-sync.index :as index]
            [logseq.db-sync.worker.http :as http]
            [promesa.core :as p]))

(def ^:private oc-page-limit 200)
(def ^:private pro-expire-seconds (* 30 24 60 60))
(def ^:private oc-managed-groups #{"alpha-tester" "beta-tester"})
(def ^:private sponsor-tier-slugs #{"sponsors" "bronze-sponsors" "gold-sponsors"})

(def ^:private collective-members-query
  "query collective($slug: String!, $limit: Int!, $offset: Int!) {\n  collective(slug: $slug) {\n    backers: members(role: BACKER, limit: $limit, offset: $offset) {\n      totalCount\n      nodes {\n        tier { slug }\n        account {\n          __typename\n          ... on Individual {\n            email\n          }\n          ... on Organization {\n            admins: members(role: ADMIN) {\n              nodes {\n                account {\n                  ... on Individual {\n                    email\n                  }\n                }\n              }\n            }\n          }\n        }\n      }\n    }\n  }\n}")

(def ^:private account-query
  "query account($slug: String!, $collectiveSlug: String!) {\n  account(slug: $slug) {\n    ... on Individual {\n      email\n    }\n    ... on Organization {\n      admins: members(role: ADMIN) {\n        nodes {\n          account {\n            ... on Individual {\n              email\n            }\n          }\n        }\n      }\n    }\n    memberOf(role: BACKER) {\n      nodes {\n        tier { slug }\n        account { slug }\n      }\n    }\n  }\n}")

(defn- now-epoch-s []
  (js/Math.floor (/ (.now js/Date) 1000)))

(defn- parse-int
  [value default]
  (cond
    (number? value) value
    (string? value) (let [n (js/parseInt value 10)]
                      (if (js/isNaN n) default n))
    :else default))

(defn- graphql-endpoint
  [^js env]
  (or (aget env "OPEN_COLLECTIVE_API_URL")
      (when-let [api-key (aget env "OPEN_COLLECTIVE_API_KEY")]
        (str "https://api.opencollective.com/graphql/v2/" api-key))))

(defn- collective-slug
  [^js env]
  (or (aget env "OPEN_COLLECTIVE_COLLECTIVE_SLUG")
      "logseq"))

(defn- webhook-token-valid?
  [^js request ^js env]
  (let [expected (aget env "OPEN_COLLECTIVE_WEBHOOK_TOKEN")
        url (js/URL. (.-url request))
        actual (.get (.-searchParams url) "token")]
    (and (string? expected)
         (seq expected)
         (= expected actual))))

(defn- ->emails
  [member-node]
  (let [account (get member-node "account")
        account-email (some-> (get account "email") string/lower-case)
        admin-emails (->> (get-in account ["admins" "nodes"])
                          (keep (fn [node]
                                  (some-> (get-in node ["account" "email"])
                                          string/lower-case))))]
    (->> (cons account-email admin-emails)
         (remove string/blank?)
         distinct
         vec)))

(defn- tiers->groups
  [tiers]
  (cond-> #{}
    (seq (set/intersection sponsor-tier-slugs tiers))
    (conj "alpha-tester")

    (contains? tiers "backers")
    (conj "beta-tester")))

(defn- merge-tier-email
  [acc member-node]
  (let [tier-slug (some-> member-node (get "tier") (get "slug"))
        emails (->emails member-node)]
    (if (and (string? tier-slug) (seq emails))
      (reduce (fn [m email]
                (update m email (fnil conj #{}) tier-slug))
              acc
              emails)
      acc)))

(defn- <graphql!
  [^js env query variables]
  (if-let [endpoint (graphql-endpoint env)]
    (p/let [resp (js/fetch endpoint
                           #js {:method "POST"
                                :headers #js {"content-type" "application/json"}
                                :body (js/JSON.stringify
                                       #js {:query query
                                            :variables (clj->js variables)})})
            text (.text resp)]
      (if-not (.-ok resp)
        (throw (ex-info "open collective request failed"
                        {:status (.-status resp)
                         :body text}))
        (let [body (js->clj (js/JSON.parse text))
              errors (get body "errors")]
          (when (seq errors)
            (throw (ex-info "open collective graphql error"
                            {:errors errors})))
          body)))
    (p/rejected (ex-info "missing open collective endpoint"
                         {:vars ["OPEN_COLLECTIVE_API_URL" "OPEN_COLLECTIVE_API_KEY"]}))))

(defn- <fetch-email->groups
  [^js env]
  (let [slug (collective-slug env)]
    (letfn [(<step [offset email->tiers]
              (p/let [body (<graphql! env collective-members-query {:slug slug
                                                                    :limit oc-page-limit
                                                                    :offset offset})
                      backers (get-in body ["data" "collective" "backers"])
                      total-count (parse-int (get backers "totalCount") 0)
                      nodes (or (get backers "nodes") [])
                      email->tiers (reduce merge-tier-email email->tiers nodes)
                      next-offset (+ offset oc-page-limit)]
                (if (< next-offset total-count)
                  (<step next-offset email->tiers)
                  (into {}
                        (map (fn [[email tiers]]
                               [email (tiers->groups tiers)]))
                        email->tiers))))]
      (<step 0 {}))))

(defn- sync-user-row-fields
  [user email->groups now-s now-ms]
  (let [email (:email user)
        current-groups (set (:user-groups user))
        target-oc-groups (get email->groups email #{})
        preserved-groups (set/difference current-groups oc-managed-groups)
        next-groups (-> (set/union preserved-groups target-oc-groups)
                        sort
                        vec)
        ;; next-is-pro? (boolean (seq target-oc-groups))
        next-is-pro? false
        next-expire-time (if next-is-pro?
                           (+ now-s pro-expire-seconds)
                           now-s)]
    {:next-fields {:expire-time next-expire-time
                   :user-groups next-groups
                   :is-pro next-is-pro?
                   :graphs-count (:graphs-count user)
                   :storage-count (:storage-count user)
                   :updated-at now-ms}
     :changed? (or (not= (set next-groups) current-groups)
                   (not= next-is-pro? (boolean (:is-pro user)))
                   (not= next-expire-time (:expire-time user)))}))

(defn <sync-existing-users!
  [^js env]
  (if-let [db (aget env "DB")]
    (p/let [email->groups (<fetch-email->groups env)
            users (index/<users-with-email db)
            now-s (now-epoch-s)
            now-ms (common/now-ms)
            update-results (p/all
                            (map (fn [user]
                                   (let [{:keys [next-fields changed?]}
                                         (sync-user-row-fields user email->groups now-s now-ms)]
                                     (if (and changed? (string? (:user-id user)))
                                       (p/let [_ (index/<user-sync-update! db (:user-id user) next-fields)]
                                         {:updated? true})
                                       (p/resolved {:updated? false}))))
                                 users))]
      {:ok true
       :users-count (count users)
       :updated-count (count (filter :updated? update-results))})
    (p/rejected (ex-info "missing DB binding"
                         {:binding "DB"}))))

(defn- <account-by-slug
  [^js env slug]
  (let [collective (collective-slug env)]
    (p/let [body (<graphql! env account-query {:slug slug
                                               :collectiveSlug collective})]
      (get-in body ["data" "account"]))))

(defn- account->email+groups
  [account collective]
  (let [tiers (->> (get-in account ["memberOf" "nodes"])
                   (filter (fn [node]
                             (= collective (get-in node ["account" "slug"]))))
                   (keep (fn [node]
                           (some-> node (get "tier") (get "slug"))) )
                   set)
        groups (tiers->groups tiers)
        emails (->> (concat [(get account "email")]
                            (map (fn [node]
                                   (get-in node ["account" "email"]))
                                 (get-in account ["admins" "nodes"])))
                    (keep (fn [email]
                            (some-> email string/lower-case)))
                    (remove string/blank?)
                    distinct
                    vec)]
    {:emails emails
     :groups groups}))

(defn <sync-user-by-slug!
  [^js env slug]
  (if-let [db (aget env "DB")]
    (let [collective (collective-slug env)]
      (p/let [account (<account-by-slug env slug)
              {:keys [emails groups]} (account->email+groups account collective)
              now-s (now-epoch-s)
              now-ms (common/now-ms)
              update-results (p/all
                              (map (fn [email]
                                     (p/let [user-id (index/<user-id-by-email db email)]
                                       (if (string? user-id)
                                         (p/let [current (index/<user-sync-info db user-id)
                                                 current-groups (set (:user-groups current))
                                                 preserved-groups (set/difference current-groups oc-managed-groups)
                                                 next-groups (-> (set/union preserved-groups groups)
                                                                 sort
                                                                 vec)
                                                 ;; next-is-pro? (boolean (seq groups))
                                                 next-is-pro? false
                                                 next-expire-time (if next-is-pro?
                                                                    (+ now-s pro-expire-seconds)
                                                                    now-s)
                                                 changed? (or (not= (set next-groups) current-groups)
                                                              (not= next-is-pro? (boolean (:is-pro current)))
                                                              (not= next-expire-time (:expire-time current)))]
                                           (if changed?
                                             (p/let [_ (index/<user-sync-update!
                                                        db
                                                        user-id
                                                        {:expire-time next-expire-time
                                                         :user-groups next-groups
                                                         :is-pro next-is-pro?
                                                         :graphs-count (:graphs-count current)
                                                         :storage-count (:storage-count current)
                                                         :updated-at now-ms})]
                                               {:updated? true})
                                             (p/resolved {:updated? false})))
                                         (p/resolved {:updated? false})) ))
                                   emails))]
        {:ok true
         :slug slug
         :email-count (count emails)
         :updated-count (count (filter :updated? update-results))}))
    (p/rejected (ex-info "missing DB binding"
                         {:binding "DB"}))))

(defn handle-webhook
  [request ^js env]
  (cond
    (not (webhook-token-valid? request env))
    (http/unauthorized)

    :else
    (-> (p/let [body (common/read-json request)
                event-type (some-> body (aget "type"))
                slug (or (some-> body (aget "data") (aget "member") (aget "memberCollective") (aget "slug"))
                         (some-> body (aget "data") (aget "collective") (aget "slug")))
                result (cond
                         (and (string? slug)
                              (contains? #{"collective.member.created"
                                           "collective.member.updated"
                                           "collective.member.deleted"}
                                         event-type))
                         (<sync-user-by-slug! env slug)

                         :else
                         (<sync-existing-users! env))
                _ (log/info :db-sync/open-collective-webhook-result result)]
          (http/json-response :worker/health {:ok true}))
        (p/catch (fn [error]
                   (log/error :db-sync/open-collective-webhook-error error)
                   (http/error-response "open collective webhook failed" 500))))))

(defn handle-scheduled
  [^js env]
  (-> (p/let [result (<sync-existing-users! env)]
        (log/info :db-sync/open-collective-weekly-sync result)
        result)
      (p/catch (fn [error]
                 (log/error :db-sync/open-collective-weekly-sync-error error)
                 nil))))
