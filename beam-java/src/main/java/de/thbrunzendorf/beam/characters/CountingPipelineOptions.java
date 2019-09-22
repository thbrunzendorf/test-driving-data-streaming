package de.thbrunzendorf.beam.characters;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public interface CountingPipelineOptions extends FlinkPipelineOptions {

    String getInputPath();

    void setInputPath(String inputPath);

    String getOutputPath();

    void setOutputPath(String outputPath);

    static CountingPipelineOptions fromArgs(String[] args) {
        return PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(CountingPipelineOptions.class);
    }
}
