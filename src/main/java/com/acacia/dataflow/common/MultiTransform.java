package com.acacia.dataflow.common;



import com.acacia.scaffolding.ITransformFactory;
import com.acacia.scaffolding.Transform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

public class MultiTransform extends PTransform<PCollection<String>, PCollection<String>> {

    List<Class<?>> classes;
    private ServiceLoader<ITransformFactory> loader;

    public MultiTransform(List<Class<?>> classes){

        this.classes = classes;
    }

    @Override
    public PCollection<String> apply(PCollection<String> item) {

        PCollection<String> tmp = item;
        loader = ServiceLoader.load(ITransformFactory.class);

        JythonFactory jf = JythonFactory.getInstance();
        ITransformFactory tf = (ITransformFactory) jf.getJythonObject(
                "com.acacia.scaffolding.ITransformFactory", "/home/bradford/proj/pypipes/acacia-common/__init__.py");
        Transform  pytrans= tf.createTransform();

        tmp = tmp.apply(ParDo.named(tmp.getName()).of(pytrans));

        Iterator<ITransformFactory> transforms = loader.iterator();
        while (transforms.hasNext()) {

                ITransformFactory f =  transforms.next();
                Transform t = f.createTransform();
                System.out.println("Applying: " + f.getClass().getCanonicalName());
                tmp = tmp.apply(ParDo.named(tmp.getName()).of(t));
        }



        return tmp;

    }

}

