(ns extract-orion
    (:require [cheshire.core :refer :all]

            )
    (:import (com.google.api.services.bigquery.model TableRow)))




(def fields
  [[:event]
   [:owts]
   [:owts_tmp_v2]
   [:page_id]
   [:page_action_id]
   [:date]
   [:timestamp]
   [:orion_writetime]
   [:client_timestamp]
   [:protocol]
   [:domain]
   [:path]
   [:data :action :name]
   [:screen :height]
   [:screen :width]
   [:source_ip]
   [:data :step]
   [:data :test_name]
   [:data :variant]
   [:data :booking_ref]
   [:data :resource_params :bkg_ref]
   [:data :resource_params :bref]
   [:data :resource_params :bkref]])

(defn create-table-row
  [^TableRow row col val]
  (.set row col val))

(defn col-to-str
  [field]
  (as-> field f
        (map name f)
        (clojure.string/join "." f)))

(defn extract
  [raw-string]
  (let [data (decode raw-string true)]
    (if (= (:service data) "webapp")
      (loop [row (new TableRow)
             item (seq fields)]
        (let [the-item (first item)
              thing (get-in data the-item)]
          (if-not the-item row
                           (recur (create-table-row row (col-to-str the-item) thing) (next item))))))))

(defn extract-clean [raw-string]
  (into {} (filter (comp not nil? val) (extract raw-string))))

(def test-string
  "{\"browser\": {\"cookie_enabled\": \"True\",\n              \"inner_height\": 714,\n              \"inner_width\": 1440,\n              \"page_height\": 714,\n              \"user_agent\": \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.122 Safari/537.36\"},\n \"client_timestamp\": 1412942308097,\n \"date\": \"2014-10-10\",\n \"domain\": \"v2staging.holidayextras.co.uk\",\n \"email\": \"andrew.castle+testtest@holidayextras.com\",\n \"event\": \"Loaded a Page\",\n \"event_type\": \"availability\",\n \"is_client\": \"True\",\n \"orion_timestamp\": \"1412942100000\",\n \"orion_uuid\": \"09084093-a810-48f6-bcde-593de9ad5867\",\n \"orion_writetime\": 1412942307234,\n \"owts\": \"75b6f1752fd946ef75c3bc4b8ed9a47e\",\n \"owts_tmp\": \"e8b0ed6a67e0a4f10384421cf2163f9b\",\n \"page_id\": \"cda1dc16c226c1a4c576504f31e9c165\",\n \"path\": \"/carpark\",\n \"port\": \"\",\n \"product_type\": \"carpark\",\n \"protocol\": \"https\",\n \"referrer\": \"http://www.holidayextras.co.uk/\",\n \"request_params\": {\"arrival_date\": \"NaN-NaN-NaN\",\n                     \"arrival_time\": \"NaN:NaN:NaN\",\n                     \"customer_ref\": \"\",\n                     \"depart\": \"LGW\",\n                     \"depart_date\": \"2014-10-17\",\n                     \"depart_time\": \"01:00:00\",\n                     \"flight\": \"TBC\",\n                     \"park_to\": \"13:00\",\n                     \"rrive\": \"\",\n                     \"term\": \"\",\n                     \"terminal\": \"\"},\n \"screen\": {\"height\": 900, \"pixelDepth\": 24, \"width\": 1440},\n \"server_ip\": \"10.210.175.195\",\n \"service\": \"webapp\",\n \"source_ip\": \"62.254.236.250\",\n \"time\": \"12:58:28\",\n \"title\": \"Parking 17 October 2014 | HolidayExtras.com\",\n \"url\": \"https://v2staging.holidayextras.co.uk/?agent=WEB1&ppcmsg=\"}")