package com.acacia.angleddream.common;


import com.acacia.sdk.AbstractTransform;
import com.acacia.sdk.AbstractTransformComposer;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

public class MultiTransform extends PTransform<PCollection<String>, PCollectionTuple> {

    private ServiceLoader<AbstractTransformComposer> loader;
    //private List<TupleTag> tagList = new ArrayList<>();

    Iterator<AbstractTransformComposer> transforms;

    private static final Logger LOG = LoggerFactory.getLogger(MultiTransform.class);

    public MultiTransform(){

        loader = ServiceLoader.load(AbstractTransformComposer.class);

        transforms = loader.iterator();

    }



    public MultiTransform(AbstractTransformComposer composer){

        List<AbstractTransformComposer> composerList = new ArrayList<>();
        composerList.add(composer);
        transforms = composerList.iterator();


    }

    @Override
    public PCollectionTuple apply(PCollection<String> item) {

        PCollection<String> tmp = item;
        PCollectionTuple results = null;



        while (transforms.hasNext()) {

            AbstractTransformComposer f =  transforms.next();

            if(f.getOrderedTransforms() != null) {

                for (AbstractTransform t : f.getOrderedTransforms()) {

                    //t.errorOutput = Tags.errorOutput; //this is weird but you gotta do it because CDF uses object identity to emit to tuple tags  https://cloud.google.com/dataflow/model/multiple-pcollections#Heterogenous
                    results = tmp.apply(ParDo.named(tmp.getName()).withOutputTags(Tags.mainOutput, TupleTagList.of(Tags.errorOutput)).of(t));

                    //           tmp = tmp.apply(ParDo.named(tmp.getName()).of(t));


                }

            }

        }



        return results;

    }

}

