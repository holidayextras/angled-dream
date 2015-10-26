package com.acacia.dataflow.common;



import com.acacia.scaffolding.AbstractTransform;
import com.acacia.scaffolding.AbstractTransformComposer;
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


//        JythonFactory jf = JythonFactory.getInstance();
//        AbstractTransformComposer tf = (AbstractTransformComposer) jf.getJythonObject(
//                "com.acacia.scaffolding.AbstractTransformComposer", "/home/bradford/proj/pypipes/acacia-common/__init__.py");
//        Transform  pytrans= tf.createTransform();

//        tmp = tmp.apply(ParDo.named(tmp.getName()).of(pytrans));

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

