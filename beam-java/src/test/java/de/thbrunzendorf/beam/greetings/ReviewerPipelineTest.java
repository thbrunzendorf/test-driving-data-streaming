package de.thbrunzendorf.beam.greetings;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReviewerPipelineTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void acceptFromListWithOneEntryMatchingExactly() {
        List<String> input = Arrays.asList("hello world!");
        PCollection<String> pipelineInput = pipeline.apply(Create.of(input));
        PCollection<String> pipelineOutput = new ReviewerPipeline().transform(pipelineInput);
        PAssert.that(pipelineOutput).containsInAnyOrder("hello world!");
        pipeline.run();
    }

    @Test
    public void acceptFromListWithOneEntryPolitely() {
        List<String> input = Arrays.asList("HELLO World!");
        PCollection<String> pipelineInput = pipeline.apply(Create.of(input));
        PCollection<String> pipelineOutput = new ReviewerPipeline().transform(pipelineInput);
        PAssert.that(pipelineOutput).containsInAnyOrder("hello world!");
        pipeline.run();
    }

    @Test
    public void acceptFromListWithOneEntryNotMatching() {
        List<String> input = Arrays.asList("Nice to meet you!");
        PCollection<String> pipelineInput = pipeline.apply(Create.of(input));
        PCollection<String> pipelineOutput = new ReviewerPipeline().transform(pipelineInput);
        PAssert.that(pipelineOutput).empty();
        pipeline.run();
    }

    @Test
    public void acceptFromListWithManyEntry() {
        List<String> input = Arrays.asList("hello world!", "Nice to meet you!", "Hello BEAM!", "How are you?");
        PCollection<String> pipelineInput = pipeline.apply(Create.of(input));
        PCollection<String> pipelineOutput = new ReviewerPipeline().transform(pipelineInput);
        PAssert.that(pipelineOutput).containsInAnyOrder("hello world!", "hello beam!");
        pipeline.run();
    }

    @Test
    public void acceptFromEmptyList() {
        List<String> input = new ArrayList<>();
        PCollection<String> pipelineInput = pipeline.apply(Create.of(input).withCoder(StringUtf8Coder.of()));
        PCollection<String> pipelineOutput = new ReviewerPipeline().transform(pipelineInput);
        PAssert.that(pipelineOutput).empty();
        pipeline.run();
    }
}
