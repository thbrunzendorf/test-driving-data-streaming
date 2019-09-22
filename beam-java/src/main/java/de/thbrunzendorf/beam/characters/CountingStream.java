package de.thbrunzendorf.beam.characters;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Character.isLetter;
import static java.lang.Character.toUpperCase;

public class CountingStream {

    public static void main(String[] args) {

        // read from input file

        // count characters

        // write to output file
    }

    public Map<Character, Long> countCharacters(List<String> input) {
        Map<Character, Long> output = input.stream().
                map(lineOfInput -> toCharacterArray(lineOfInput.toCharArray())).
                flatMap(character -> Arrays.stream(character)).
                filter(character -> isLetter(character)).
                map(character -> toUpperCase(character)).
                collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        return output;
    }

    private static Character[] toCharacterArray(char[] charArray) {
        Character[] characterArray = new Character[charArray.length];
        for (int i = 0; i < charArray.length; i++) {
            characterArray[i] = Character.valueOf(charArray[i]);
        }
        return characterArray;
    }
}
