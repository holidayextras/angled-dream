package com.acacia.dataflow.common;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.DirectPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsValidator;
import com.google.cloud.dataflow.sdk.runners.*;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;

public class ComposerDataflowPipelineRunner extends DataflowPipelineRunner {


    private DataflowPipelineRunner dpr;

    protected ComposerDataflowPipelineRunner(DataflowPipelineOptions options) {
        super(options);
        this.dpr = DataflowPipelineRunner.fromOptions(options);
    }

    /**
     * Applies the given transform to the input. For transforms with customized definitions
     * for the Dataflow pipeline runner, the application is intercepted and modified here.
     */
    @Override
    public <OutputT extends POutput, InputT extends PInput> OutputT apply(
            PTransform<InputT, OutputT> transform, InputT input) {

        if (Combine.GroupedValues.class.equals(transform.getClass())
                || GroupByKey.class.equals(transform.getClass())) {

            // For both Dataflow runners (streaming and batch), GroupByKey and GroupedValues are
            // primitives. Returning a primitive output instead of the expanded definition
            // signals to the translator that translation is necessary.
            @SuppressWarnings("unchecked")
            PCollection<?> pc = (PCollection<?>) input;
            @SuppressWarnings("unchecked")
            OutputT outputT = (OutputT) PCollection.createPrimitiveOutputInternal(
                    pc.getPipeline(),
                    transform instanceof GroupByKey
                            ? ((GroupByKey<?, ?>) transform).updateWindowingStrategy(pc.getWindowingStrategy())
                            : pc.getWindowingStrategy(),
                    pc.isBounded());
            return outputT;

        } else {
            return transform.apply(input);
        }
    }


    @Override
    public DataflowPipelineJob run(Pipeline pipeline) {
        return dpr.run(pipeline);
    }

    @Override
    public DataflowPipelineTranslator getTranslator() {
        return dpr.getTranslator();
    }

    @Override
    public void setHooks(DataflowPipelineRunnerHooks hooks) {
        dpr.setHooks(hooks);
    }

    @Override
    public String toString() {
        return dpr.toString();
    }

    public static ComposerDataflowPipelineRunner fromOptions(PipelineOptions o){
        ComposerDataflowPipelineRunner c = new ComposerDataflowPipelineRunner(o.as(DataflowPipelineOptions.class)); //fuck everything about this
        c.dpr = DataflowPipelineRunner.fromOptions(o.as(DataflowPipelineOptions.class));
        return c;
    }

}
