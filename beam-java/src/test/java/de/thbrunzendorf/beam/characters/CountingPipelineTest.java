package de.thbrunzendorf.beam.characters;

import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertTrue;

public class CountingPipelineTest implements Serializable {

    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    @Before
    public void setUp() throws Exception {
        CoderRegistry coderRegistry = p.getCoderRegistry();
        coderRegistry.registerCoderForClass(Character.class, CharacterCoder.of());
    }

    @Test
    public void firstAsserts() {
        PCollection<String> pInput = p.apply(Create.empty(TypeDescriptors.strings()));
        PCollection<KV<Character, Long>> pOutput = new CountingPipeline().countCharacters(pInput);
        //assertThat(pOutput, is(anEmptyMap()));
        assertThat(pOutput, is(notNullValue()));
        assertTrue(pOutput.isBounded().equals(PCollection.IsBounded.BOUNDED));
        assertTrue(pOutput.getWindowingStrategy().equals(WindowingStrategy.globalDefault()));
        PAssert.that(pOutput).empty();
        p.run();
    }

    @Test
    public void countsCharactersFromEmptyList() {
        List<String> input = new ArrayList();
        PCollection<String> pInput = p.apply(Create.of(input).withCoder(StringUtf8Coder.of()));
        PCollection<KV<Character, Long>> pOutput = new CountingPipeline().countCharacters(pInput);
        PAssert.that(pOutput).empty();
        p.run();
    }

    @Test
    public void countsCharactersFromListWithOneEntryWithOneCharacter() {
        List<String> input = Arrays.asList("a");
        PCollection<String> pInput = p.apply(Create.of(input).withCoder(StringUtf8Coder.of()));
        PCollection<KV<Character, Long>> pOutput = new CountingPipeline().countCharacters(pInput);
        PAssert.that(pOutput).containsInAnyOrder(KV.of('A', 1L));
        p.run();
    }

    @Test
    public void countsCharactersFromListWithMultipleEntriesWithMultipleCharacters() {
        List<String> input = Arrays.asList("Hello", "World!");
        PCollection<String> pInput = p.apply(Create.of(input).withCoder(StringUtf8Coder.of()));
        PCollection<KV<Character, Long>> pOutput = new CountingPipeline().countCharacters(pInput);
        PAssert.that(pOutput).containsInAnyOrder(
                KV.of('H', 1L), KV.of('E', 1L), KV.of('L', 3L), KV.of('O', 2L),
                KV.of('W', 1L), KV.of('R', 1L), KV.of('D', 1L));
        p.run();
    }
}
