package com.acacia.sdk;

import org.python.core.*;
import org.python.util.PythonInterpreter;

import java.util.Map;

public class TestPy {


    static String data = "{\"resource\": {\"domain\": \"v2.holidayextras.co.uk\", \"is_client\": true, \"client_timestamp\": 1452291935334, \"event\": \"load\", \"orion_server_date\": \"2016-01-08\", \"service\": \"webapp\", \"orion_timestamp\": \"1452291900000\", \"environment\": \"production\", \"page_type\": \"availability\", \"server_ip\": \"172.20.80.102\", \"owts\": \"a725f01f0c9c114fb438e89eeb3d9b7a\", \"launch_id\": \"418025\", \"customer_id\": \"\", \"page_id\": \"df489a96ef328fdb3c2cb6f121f0c8fc\", \"email\": \"james@jekab.com\", \"orion_writetime\": 1452291927512, \"orion_uuid\": \"592b60b5-80f0-4b59-8859-6174ab8db21b\", \"screen\": {\"width\": 1280, \"pixelDepth\": 24, \"height\": 1024}, \"resource_location\": {\"domain\": \"v2.holidayextras.co.uk\", \"protocol\": \"https\", \"search\": \"?agent=WK787&ppcmsg=&lang=en\", \"path\": \"/carpark\", \"port\": \"\", \"hash\": \"#/carpark?arrive=&campaign_id=hdayextras.714605&customer_ref=&depart=STN&flight=&in=2016-01-19&launch_id=418025&out=2016-01-16&park_from=04:30&park_to=23:30&term=&terminal=\"}, \"date\": \"2016-01-08\", \"data\": {\"hxtrack\": {}, \"sort_criteria\": \"recommended\", \"resource_params\": {\"lang\": \"en\", \"term\": \"\", \"depart\": \"STN\", \"launch_id\": \"418025\", \"campaign_id\": \"hdayextras.714605\", \"agent\": \"WK787\", \"terminal\": \"\", \"arrival_time\": \"04:30\", \"arrival_date\": \"2016-01-19\", \"flight\": \"\", \"customer_ref\": \"\", \"arrive\": \"\", \"ppcmsg\": \"\", \"depart_time\": \"23:30\", \"depart_date\": \"2016-01-16\"}, \"sort_order\": \"asc\", \"products\": [{\"original_position\": 1, \"display_position\": 1, \"price\": 3869, \"code\": \"HPSTN5\", \"merch_position\": 1}, {\"original_position\": 2, \"display_position\": 2, \"price\": 4319, \"code\": \"HPSTV5\", \"merch_position\": 2}, {\"original_position\": 3, \"display_position\": 3, \"price\": 4139, \"code\": \"HPSTV6\", \"merch_position\": 3}, {\"original_position\": 4, \"display_position\": 4, \"price\": 4139, \"code\": \"HPSTN6\", \"merch_position\": 4}, {\"original_position\": 5, \"display_position\": 5, \"price\": 5219, \"code\": \"HPSTX9\", \"merch_position\": 5}, {\"original_position\": 6, \"display_position\": 6, \"price\": 3599, \"code\": \"HPSTZ9\", \"merch_position\": 6}, {\"original_position\": 7, \"display_position\": 7, \"price\": 4249, \"code\": \"HPSTZ6\", \"merch_position\": 7}], \"location\": \"STN\"}, \"orion_server_time\": \"22:25:27\", \"product_type\": \"carpark\", \"source_ip\": \"82.46.248.22\", \"time\": \"22:25:35\", \"owts_tmp_v2\": \"4febd0d0680b5cfbf471d691698f2d46\", \"browser\": {\"inner_height\": 785, \"inner_width\": 1194, \"user_agent\": \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36\", \"page_height\": 785, \"cookie_enabled\": true}}}";
    static String str2 ="{\"resource\": {\"domain\": \"v2.holidayextras.co.uk\", \"is_client\": true, \"client_timestamp\": 1452291935334, \"event\": \"load\", \"orion_server_date\": \"2016-01-08\", \"service\": \"webapp\", \"orion_timestamp\": \"1452291900000\", \"environment\": \"production\", \"page_type\": \"availability\", \"server_ip\": \"172.20.80.102\", \"owts\": \"a725f01f0c9c114fb438e89eeb3d9b7a\", \"launch_id\": \"418025\", \"customer_id\": \"\", \"page_id\": \"df489a96ef328fdb3c2cb6f121f0c8fc\", \"email\": \"james@jekab.com\", \"orion_writetime\": 1452291927512, \"orion_uuid\": \"592b60b5-80f0-4b59-8859-6174ab8db21b\", \"screen\": {\"width\": 1280, \"pixelDepth\": 24, \"height\": 1024}, \"resource_location\": {\"domain\": \"v2.holidayextras.co.uk\", \"protocol\": \"https\", \"search\": \"?agent=WK787&ppcmsg=&lang=en\", \"path\": \"/carpark\", \"port\": \"\", \"hash\": \"#/carpark?arrive=&campaign_id=hdayextras.714605&customer_ref=&depart=STN&flight=&in=2016-01-19&launch_id=418025&out=2016-01-16&park_from=04:30&park_to=23:30&term=&terminal=\"}, \"date\": \"2016-01-08\", \"data\": {\"hxtrack\": {}, \"sort_criteria\": \"recommended\", \"resource_params\": {\"lang\": \"en\", \"term\": \"\", \"depart\": \"STN\", \"launch_id\": \"418025\", \"campaign_id\": \"hdayextras.714605\", \"agent\": \"WK787\", \"terminal\": \"\", \"arrival_time\": \"04:30\", \"arrival_date\": \"2016-01-19\", \"flight\": \"\", \"customer_ref\": \"\", \"arrive\": \"\", \"ppcmsg\": \"\", \"depart_time\": \"23:30\", \"depart_date\": \"2016-01-16\"}, \"sort_order\": \"asc\", \"products\": [{\"original_position\": 1, \"display_position\": 1, \"price\": 3869, \"code\": \"HPSTN5\", \"merch_position\": 1}, {\"original_position\": 2, \"display_position\": 2, \"price\": 4319, \"code\": \"HPSTV5\", \"merch_position\": 2}, {\"original_position\": 3, \"display_position\": 3, \"price\": 4139, \"code\": \"HPSTV6\", \"merch_position\": 3}, {\"original_position\": 4, \"display_position\": 4, \"price\": 4139, \"code\": \"HPSTN6\", \"merch_position\": 4}, {\"original_position\": 5, \"display_position\": 5, \"price\": 5219, \"code\": \"HPSTX9\", \"merch_position\": 5}, {\"original_position\": 6, \"display_position\": 6, \"price\": 3599, \"code\": \"HPSTZ9\", \"merch_position\": 6}, {\"original_position\": 7, \"display_position\": 7, \"price\": 4249, \"code\": \"HPSTZ6\", \"merch_position\": 7}], \"location\": \"STN\"}, \"orion_server_time\": \"22:25:27\", \"product_type\": \"carpark\", \"source_ip\": \"82.46.248.22\", \"time\": \"22:25:35\", \"owts_tmp_v2\": \"4febd0d0680b5cfbf471d691698f2d46\", \"browser\": {\"inner_height\": 785, \"inner_width\": 1194, \"user_agent\": \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36\", \"page_height\": 785, \"cookie_enabled\": true}}}";


    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws PyException {

        // Create an instance of the PythonInterpreter
        PythonInterpreter interp = new PythonInterpreter();



//        // The exec() method executes strings of code
//        interp.exec("import sys");
//        interp.exec("print sys");
//
//        // Set variable values within the PythonInterpreter instance
//        interp.set("a", new PyInteger(42));
//        interp.exec("print a");
//        interp.exec("x = 2+2");
//
//        // Obtain the value of an object from the PythonInterpreter and store it
//        // into a PyObject.
//        PyObject x = interp.get("x");
//        System.out.println("x: " + x);

        interp.compile("table_last_search_info", "/home/bradford/proj/sinksponsys/sinksponsys/get_LAST_SEARCH_INFO.py");
        interp.exec("import sys");
        interp.exec("sys.path.append(\"/home/bradford/proj/sinksponsys/sinksponsys\")");

        interp.exec("import table_last_search_info");

        interp.set("out", new PyString());
        interp.set("inval", new PyString(str2));
        interp.exec("out = table_last_search_info.build_hot_leads_row_from_str(inval)");



        String item = interp.get("out").toString();

        System.out.println("Mapout " + item);



    }




}
