package de.thbrunzendorf.beam.characters;

import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class CountingPipelineWrapperTest implements Serializable {

    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    @Test
    public void countsCharactersFromEmptyList() {
        List<String> input = new ArrayList();
        Map<Character, Long> output = new CountingPipelineWrapper().countCharacters(input);
        assertThat(output, is(anEmptyMap()));
    }

    @Test
    public void countsCharactersFromListWithOneEntryWithOneCharacter() {
        List<String> input = Arrays.asList("a");
        Map<Character, Long> output = new CountingPipelineWrapper().countCharacters(input);
        assertThat(output, is(aMapWithSize(1)));
        assertThat(output, hasEntry('A', 1L));
    }

    @Test
    public void countsCharactersFromListWithMultipleEntriesWithMultipleCharacters() {
        List<String> input = Arrays.asList("Hello", "World!");
        Map<Character, Long> output = new CountingPipelineWrapper().countCharacters(input);
        assertThat(output, is(aMapWithSize(7)));
        assertThat(output, hasEntry('H', 1L));
        assertThat(output, hasEntry('E', 1L));
        assertThat(output, hasEntry('L', 3L));
        assertThat(output, hasEntry('O', 2L));
        assertThat(output, hasEntry('W', 1L));
        assertThat(output, hasEntry('R', 1L));
        assertThat(output, hasEntry('D', 1L));
    }

    private static Map<Character, Long> outputMap = new ConcurrentHashMap<>();

    class CountingPipelineWrapper implements Serializable {

        private Map<Character, Long> countCharacters(List<String> input) {
            outputMap.clear();
            CoderRegistry coderRegistry = p.getCoderRegistry();
            coderRegistry.registerCoderForClass(Character.class, CharacterCoder.of());
            PCollection<String> pInput = p.apply(Create.of(input).withCoder(StringUtf8Coder.of()));
            PCollection<KV<Character, Long>> pOutput = new CountingPipeline().countCharacters(pInput);
            pOutput.apply(new WriteStaticMap());
            p.run();
            return outputMap;
        }
    }

    class WriteStaticMap extends PTransform<PCollection<KV<Character, Long>>, PDone> {

        PTransform inner = ParDo.of(new DoFn<KV<Character, Long>, KV<Character, Long>>() {
            @ProcessElement
            public void processElement(@Element KV<Character, Long> input, OutputReceiver<KV<Character, Long>> receiver) {
                outputMap.put(input.getKey(), input.getValue());
                receiver.output(input);
            }
        });

        @Override
        public PDone expand(PCollection<KV<Character, Long>> input) {
            inner.expand(input);
            return PDone.in(input.getPipeline());
        }
    }
}
