package de.thbrunzendorf.beam.characters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Character.isLetter;
import static java.lang.Character.toUpperCase;

public class CountingStream {

    public static void main(String[] args) {
        CountingStream countingStream = new CountingStream();
        String inputFileName = "build/resources/test/counting-input.txt";
        String outputFileName = "build/resources/test/counting-output.txt";
        try {
            Stream<String> stream = Files.lines(Paths.get(inputFileName));
            Map<Character, Long> characterMap = countingStream.countCharacters(stream);
            List<String> list = countingStream.formatCharacterCounts(characterMap);
            Files.write(Paths.get(outputFileName), list);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<Character, Long> countCharacters(List<String> input) {
        Stream<String> stringStream = input.stream();
        Map<Character, Long> output = countCharacters(stringStream);
        return output;
    }

    private Map<Character, Long> countCharacters(Stream<String> stringStream) {
        return stringStream.
                map(lineOfInput -> toCharacterArray(lineOfInput.toCharArray())).
                flatMap(character -> Arrays.stream(character)).
                filter(character -> isLetter(character)).
                map(character -> toUpperCase(character)).
                collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    private static Character[] toCharacterArray(char[] charArray) {
        Character[] characterArray = new Character[charArray.length];
        for (int i = 0; i < charArray.length; i++) {
            characterArray[i] = Character.valueOf(charArray[i]);
        }
        return characterArray;
    }

    private List<String> formatCharacterCounts(Map<Character, Long> characterMap) {
        List<String> list = new ArrayList<>();
        for (Map.Entry<Character, Long> entry : characterMap.entrySet()) {
            list.add("(" + entry.getKey().toString() + "," + entry.getValue().toString() + ")");
        }
        return list;
    }
}
