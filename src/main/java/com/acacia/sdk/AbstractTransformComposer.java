package com.acacia.sdk;


import java.util.List;
import java.util.Map;

public abstract class AbstractTransformComposer {

    public AbstractTransformComposer(){}

    public AbstractTransformComposer(Map<String, Object> args){};

    public abstract List<AbstractTransform> getOrderedTransforms();

}
