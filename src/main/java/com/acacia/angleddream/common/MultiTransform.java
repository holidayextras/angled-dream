package com.acacia.angleddream.common;


import com.acacia.sdk.AbstractTransform;
import com.acacia.sdk.AbstractTransformComposer;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

public class MultiTransform extends PTransform<PCollection<String>, PCollection<String>> {

    List<Class<?>> classes;
    private ServiceLoader<AbstractTransformComposer> loader;

    public MultiTransform(){

        loader = ServiceLoader.load(AbstractTransformComposer.class);

    }

    @Override
    public PCollection<String> apply(PCollection<String> item) {

        PCollection<String> tmp = item;


        Iterator<AbstractTransformComposer> transforms = loader.iterator();
        while (transforms.hasNext()) {

            AbstractTransformComposer f =  transforms.next();

            for(AbstractTransform t : f.getOrderedTransforms()) {

                System.out.println("Applying: " + t.getClass().getCanonicalName());
                tmp = tmp.apply(ParDo.named(tmp.getName()).of(t));
            }

        }



        return tmp;

    }

}

