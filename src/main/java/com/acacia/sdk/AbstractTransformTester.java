package com.acacia.sdk;

import com.google.cloud.dataflow.sdk.transforms.DoFnTester;

public class AbstractTransformTester
{

    AbstractTransformComposer c;

    public AbstractTransformTester(AbstractTransformComposer c){
        this.c = c;
    }


    public String apply(String input){

        for(AbstractTransform t : c.getOrderedTransforms()){

            try {
                input = t.transform(input);
            } catch (GenericDataflowAppException e) {
                e.printStackTrace();
            }

        }

        return input;

    }


}
