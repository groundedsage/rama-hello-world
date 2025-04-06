(ns examples.tutorial.hello-world 
  "https://redplanetlabs.com/docs/~/tutorial1.html#_running_a_module_on_a_cluster"
  (:use [com.rpl.rama])
  (:require [com.rpl.rama.test :as rtest]
            [com.rpl.rama.ops :as ops]))

(defmodule HelloWorldModule
  [setup topologies]
  (declare-depot setup *depot :random)
  (let [s (stream-topology topologies "s")]
    (<<sources s
               (source> *depot :> *data)
               (println *data))))

(comment
  
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc HelloWorldModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name HelloWorldModule)
          depot (foreign-depot ipc module-name "*depot")]
      (foreign-append! depot "Hello, world!!")))

  )