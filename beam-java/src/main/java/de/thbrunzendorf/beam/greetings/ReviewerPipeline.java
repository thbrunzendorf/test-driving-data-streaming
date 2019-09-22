package de.thbrunzendorf.beam.greetings;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class ReviewerPipeline {

    public static void main(String[] args) {

        // create pipeline
        Pipeline pipeline = Pipeline.create();

        // read from input file
        PCollection<String> pipelineInput = pipeline
                .apply(TextIO
                        .read()
                        .from("build/resources/test/greetings-input.txt"));

        // transform
        PCollection<String> pipelineOutput = new ReviewerPipeline().transform(pipelineInput);

        // write to output file
        pipelineOutput
                .apply(TextIO
                        .write()
                        .to("build/resources/test/greetings-output.txt"));

        // run pipeline
        pipeline.run();
    }

    public PCollection<String> transform(PCollection<String> pipelineInput) {
        return pipelineInput
                .apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via(String::toLowerCase))
                .apply(Filter
                        .by(s -> s.startsWith("hello")));
    }
}
