package de.thbrunzendorf.beam.greetings;

import java.util.List;
import java.util.stream.Collectors;

public class ReviewerStream {
    public List<String> accept(List<String> input) {
        return input.stream()
                .map(String::toLowerCase)
                .filter(s -> s.startsWith("hello"))
                .collect(Collectors.toList());
    }
}
