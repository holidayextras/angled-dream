package com.acacia.sdk;


import com.acacia.angleddream.common.Tags;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public abstract class AbstractTransform  {




    //function to transform a string, used by processElement



    public String transform(String input) throws GenericDataflowAppException {return input;}



    public String transform(String input, Map<String,String> args) throws GenericDataflowAppException {return input;}






}
