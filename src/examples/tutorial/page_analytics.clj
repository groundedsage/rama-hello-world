(ns examples.tutorial.page-analytics 
  (:use [com.rpl.rama]
        com.rpl.rama.path)
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.test :as rtest]
            [com.rpl.rama.ops :as ops]
            [hyperfiddle.rcf :as rcf]))


(defmodule PageAnalyticsModule 
  [setup topologies]
  (declare-depot setup *depot :random)
  (let [s (stream-topology topologies "s")]
    (declare-pstate s $$pageViewCount {String Long})
    (declare-pstate s $$sessionHistory {String [(map-schema String Object)]})
    
    (<<sources s
               (source> *depot :> *pageVisit)
               (local-select> "sessionId" *pageVisit :> *sessionId)
               (local-select> "path" *pageVisit :> *path) 
               (+compound $$pageViewCount {*path (aggs/+count)})

               (<<ramafn %thin-visit [*v] (:> (conj *v (dissoc *pageVisit "sessionId"))))
               (local-transform> [(keypath *sessionId) (nil->val []) (term %thin-visit)] $$sessionHistory))))



(comment 
  
  (rcf/enable!)

  (rcf/tests 
   (with-open [ipc (rtest/create-ipc)]
     (rtest/launch-module! ipc PageAnalyticsModule {:tasks 1 :threads 1})
     (let [module-name (get-module-name PageAnalyticsModule)
           depot (foreign-depot ipc module-name "*depot")
           page-view-count (foreign-pstate ipc module-name "$$pageViewCount")
           session-history (foreign-pstate ipc module-name "$$sessionHistory")]
       (let [page-visit {"sessionId" "abc123"
                         "path" "/posts"
                         "duration" 4200}]
         (foreign-append! depot page-visit))

       ;; tests    
       (foreign-select-one (keypath "/posts") page-view-count) := 1
       (foreign-select-one (keypath "abc123") session-history) := [{"path" "/posts" "duration" 4200}])
     nil))
  
  )