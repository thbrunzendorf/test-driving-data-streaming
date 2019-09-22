package de.thbrunzendorf.beam.characters;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class CountingStreamTest {

    @Test
    public void countsCharactersFromEmptyList() {
        List<String> input = new ArrayList();
        Map<Character, Long> output = new CountingStream().countCharacters(input);
        assertThat(output, is(anEmptyMap()));
    }

    @Test
    public void countsCharactersFromListWithOneEntryWithOneCharacter() {
        List<String> input = Arrays.asList("a");
        Map<Character, Long> output = new CountingStream().countCharacters(input);
        assertThat(output, is(aMapWithSize(1)));
        assertThat(output, hasEntry('A', 1L));
    }

    @Test
    public void countsCharactersFromListWithMultipleEntriesWithMultipleCharacters() {
        List<String> input = Arrays.asList("Hello", "World!");
        Map<Character, Long> output = new CountingStream().countCharacters(input);
        assertThat(output, is(aMapWithSize(7)));
        assertThat(output, hasEntry('H', 1L));
        assertThat(output, hasEntry('E', 1L));
        assertThat(output, hasEntry('L', 3L));
        assertThat(output, hasEntry('O', 2L));
        assertThat(output, hasEntry('W', 1L));
        assertThat(output, hasEntry('R', 1L));
        assertThat(output, hasEntry('D', 1L));
    }
}
