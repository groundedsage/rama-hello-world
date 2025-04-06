(ns examples.tutorial.word-count 
  (:use [com.rpl.rama]
        com.rpl.rama.path)
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.test :as rtest]
            [com.rpl.rama.ops :as ops]
            [hyperfiddle.rcf :as rcf]))

(defmodule SimpleWordCountModule
  [setup topologies]
  (declare-depot setup *wordDepot :random)
  (let [s (stream-topology topologies "wordCountStream")]
    (declare-pstate s $$wordCounts {String Long})
    (<<sources s
               (source> *wordDepot :> *token)
               (|hash *token)
               (+compound $$wordCounts {*token (aggs/+count)}))))

(comment

  (rcf/enable!)
  ;; Test code
  (rcf/tests
   (with-open [ipc (rtest/create-ipc)]
     (rtest/launch-module! ipc SimpleWordCountModule {:tasks 1 :threads 1})
     (let [module-name (get-module-name SimpleWordCountModule)
           depot (foreign-depot ipc module-name "*wordDepot")
           wc (foreign-pstate ipc module-name "$$wordCounts")]
       (foreign-append! depot "one")
       (foreign-append! depot "two")
       (foreign-append! depot "two")
       (foreign-append! depot "three")
       (foreign-append! depot "three")
       (foreign-append! depot "three")

       ;; test the word counts    
       (foreign-select-one (keypath "one") wc) := 1
       (foreign-select-one (keypath "two") wc) := 2
       (foreign-select-one (keypath "three") wc) := 3)))
  
  )
