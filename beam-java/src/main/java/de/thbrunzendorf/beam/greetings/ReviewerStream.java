package de.thbrunzendorf.beam.greetings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class ReviewerStream {

    public static void main(String[] args) {
        try {
            // read from input file
            List<String> inputList = Files.lines(Paths.get("build/resources/test/greetings-input.txt"))
                    .collect(Collectors.toList());
            // process
            List<String> outputList = new ReviewerStream().accept(inputList);
            // write to output file
            Files.write(Paths.get("build/resources/test/greetings-output.txt"), outputList);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> accept(List<String> inputList) {
        return inputList.stream()
                .map(String::toLowerCase)
                .filter(s -> s.startsWith("hello"))
                .collect(Collectors.toList());
    }
}
