package com.acacia.angleddream.common;

import com.acacia.sdk.AbstractTransformComposer;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.ServiceLoader;

public class Tags {


    //from the SDK: To aid in assigning default Coders for results of side outputs of ParDo, an output TupleTag should be instantiated with an extra {} so it is an instance of an anonymous subclass without generic type parameters. Input TupleTags require no such extra instantiation (although it doesn't hurt). For example:

    public static final TupleTag<String> mainOutput = new TupleTag<String>("mainOutput"){};
    public static final TupleTag<String> errorOutput = new TupleTag<String>("errorOutput"){};



}
