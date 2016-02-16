package com.acacia.angleddream.common;


import com.acacia.sdk.AbstractTransform;
import com.acacia.sdk.AbstractTransformComposer;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.*;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.*;
import org.python.antlr.op.Mult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MultiTransform extends PTransform<PCollection<String>, PCollectionTuple> {

    private ServiceLoader<AbstractTransformComposer> loader;
    //private List<TupleTag> tagList = new ArrayList<>();



    Iterator<AbstractTransformComposer> transforms;

    private static final Logger LOG = LoggerFactory.getLogger(MultiTransform.class);

    private static Map<String,String> args = null;

    private static final Coder<KV<String, String>> TEST_CODER =
            KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());


    Pipeline p = null;

    public MultiTransform(){

        loader = ServiceLoader.load(AbstractTransformComposer.class);
        transforms = loader.iterator();

    }
    public MultiTransform(Map<String,String> args){

        loader = ServiceLoader.load(AbstractTransformComposer.class);
        transforms = loader.iterator();
        MultiTransform.args = args;

    }


    public MultiTransform(AbstractTransformComposer composer){

        List<AbstractTransformComposer> composerList = new ArrayList<>();
        composerList.add(composer);
        transforms = composerList.iterator();


    }


    public MultiTransform(AbstractTransformComposer composer, Map<String,String> args){

        List<AbstractTransformComposer> composerList = new ArrayList<>();
        composerList.add(composer);
        transforms = composerList.iterator();
        MultiTransform.args = args;

    }

    @Override
    public PCollectionTuple apply(PCollection<String> item) {

        //do these here because deserialization?
        loader = ServiceLoader.load(AbstractTransformComposer.class);

        transforms = loader.iterator();

        PCollection<String> tmp = item;
        PCollectionTuple results = null;



        //we want to use args as options from the CLI passed all the way to a abstracttransform



        while (transforms.hasNext()) {

            AbstractTransformComposer f =  transforms.next();

            if(f.getOrderedTransforms() != null) {

                for (AbstractTransform t : f.getOrderedTransforms()) {

                    //t.errorOutput = Tags.errorOutput; //this is weird but you gotta do it because CDF uses object identity to emit to tuple tags  https://cloud.google.com/dataflow/model/multiple-pcollections#Heterogenous

                    if(Tags.argsView != null){
                        System.out.println("has args");
                        results = tmp.apply(ParDo.named(tmp.getName()).withOutputTags(Tags.mainOutput, TupleTagList.of(Tags.errorOutput)).withSideInputs(Tags.argsView).of(t));
                    }
                    else {
                        results = tmp.apply(ParDo.named(tmp.getName()).withOutputTags(Tags.mainOutput, TupleTagList.of(Tags.errorOutput)).of(t));
                    }

                    //           tmp = tmp.apply(ParDo.named(tmp.getName()).of(t));
                }
            }
        }

        return results;

    }

}

