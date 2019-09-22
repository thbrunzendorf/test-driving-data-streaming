package de.thbrunzendorf.beam.greetings;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;

public class ReviewerStreamTest {
    @Test
    public void acceptFromListWithOneEntryMatchingExactly() {
        List<String> input = Arrays.asList("hello world!");
        List<String> output = new ReviewerStream().accept(input);
        assertThat(output, Matchers.containsInAnyOrder("hello world!"));
    }

    @Test
    public void acceptFromListWithOneEntryPolitely() {
        List<String> input = Arrays.asList("HELLO World!");
        List<String> output = new ReviewerStream().accept(input);
        assertThat(output, Matchers.containsInAnyOrder("hello world!"));
    }

    @Test
    public void acceptFromListWithOneEntryNotMatching() {
        List<String> input = Arrays.asList("Nice to meet you!");
        List<String> output = new ReviewerStream().accept(input);
        assertThat(output, Matchers.empty());
    }

    @Test
    public void acceptFromListWithManyEntry() {
        List<String> input = Arrays.asList("hello world!", "Nice to meet you!", "Hello BEAM!", "How are you?");
        List<String> output = new ReviewerStream().accept(input);
        assertThat(output, Matchers.containsInAnyOrder("hello world!", "hello beam!"));
    }

    @Test
    public void acceptFromEmptyList() {
        List<String> input = Arrays.asList();
        List<String> output = new ReviewerStream().accept(input);
        assertThat(output, Matchers.empty());
    }
}
