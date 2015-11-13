(ns extract-orion
    (:require [cheshire.core :refer :all]

            )
    (:import (com.google.api.services.bigquery.model TableRow)))




;parse graph
(def fields
  [["event"]
   ["owts"]
   ["owts_tmp_v2"]
   ["page_id"]
   ["page_action_id"]
   ["date"]
   ["timestamp"]
   ["orion_writetime"]
   ["client_timestamp"]
   ["protocol"]
   ["domain"]
   ["path"]

   ["screen" "height"]
   ["screen" "width"]
   ["source_ip"]

   ;["data" "action" "name"]
   ;["data" "step"]
   ;["data" "test_name"]
   ;["data" "variant"]
   ;["data" "booking_ref"]
   ;["data" "resource_params" "bkg_ref"]
   ;["data" "resource_params" "bref"]
   ;["data" "resource_params" "bkref"]

   ])

(defn mm
  [coll]
  (mapv (fn [v] [(apply merge v)]) coll)
  )


(defn extract
  [raw-string]
  (let [data (decode raw-string)]
    (if (= (get data "service") "webapp")
      (let [lv1 (apply merge (mapv #(select-keys data %) fields))
            lv2 {
                 "screen" {"height" (get-in data ["screen" "height"])
                           "width" (get-in data ["screen" "height"])}
                 "data" { "step" (get-in data ["data" "step"])
                         "test_name" (get-in data ["data" "test_name"])
                         "variant" (get-in data ["data" "variant"])
                         "booking_ref" (get-in data ["data" "booking_ref"])
                         "resource_params" {"bkg_ref" (get-in data ["data" "resource_params" "bkg_ref"])
                                            "bref" (get-in data ["data" "resource_params" "bref"])
                                            "bkref" (get-in data ["data" "resource_params" "bkref"])
                                            }
                         "action" { "name" (get-in data ["data" "action" "name"])
                                   }
                         }

                 }
            ]

        (apply merge lv1 lv2)
        )

      )
    )

  )