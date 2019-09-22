package de.thbrunzendorf.beam.characters;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public class CountingPipeline implements Serializable {

    public static void main(String[] args) {
        CountingPipeline countingPipeline = new CountingPipeline();
        CountingPipelineOptions options = CountingPipelineOptions.fromArgs(args);
        System.out.println(options.getRunner().getName());
        Pipeline pipeline = Pipeline.create(options);

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForClass(Character.class, CharacterCoder.of());

        countingPipeline.run(pipeline, options.getInputPath(), options.getOutputPath());
    }

    private void run(Pipeline pipeline, String inputPath, String outputPath) {
        PCollection<String> linesOfInput = pipeline.apply("ReadFromInputTextFile", TextIO.read().from(inputPath));
        PCollection<KV<Character, Long>> characterCounts = linesOfInput.apply(new CountCharactersTransform());
        PCollection<String> outputLines = characterCounts.apply(ParDo.of(new FormatCharacterCounts()));
        outputLines.apply("WriteToOutputTextFile", TextIO.write().to(outputPath));
        pipeline.run();
    }

    public PCollection<KV<Character, Long>> countCharacters(PCollection<String> linesOfInput) {
        return linesOfInput
                .apply(ParDo
                        .of(new StringToCharacters()))
                .apply(Filter
                        .by(Character::isLetter))
                .apply(MapElements
                        .into(TypeDescriptors.characters())
                        .via(Character::toUpperCase))
                .apply(Count
                        .perElement());
    }
}

class CountCharactersTransform extends PTransform<PCollection<String>, PCollection<KV<Character, Long>>> {

    @Override
    public PCollection<KV<Character, Long>> expand(PCollection<String> input) {
        return new CountingPipeline().countCharacters(input);
    }
}

class CharacterCoder extends AtomicCoder<Character> {

    private static final CharacterCoder INSTANCE = new CharacterCoder();

    public static CharacterCoder of() {
        return INSTANCE;
    }

    private CharacterCoder() {
    }

    @Override
    public void encode(Character value, OutputStream outStream) throws CoderException, IOException {
        int intValue = (int) value;
        outStream.write(intValue);
    }

    @Override
    public Character decode(InputStream inStream) throws CoderException, IOException {
        int intValue = inStream.read();
        return (char) intValue;
    }
}

class StringToCharacters extends DoFn<String, Character> {

    @ProcessElement
    public void processElement(@Element String lineOfInput, OutputReceiver<Character> receiver) {
        for (Character character : lineOfInput.toCharArray()) {
            receiver.output(character);
        }
    }
}

class FormatCharacterCounts extends DoFn<KV<Character, Long>, String> {

    @ProcessElement
    public void processElement(@Element KV<Character, Long> kv, OutputReceiver<String> receiver) {
        String s = "(" + kv.getKey().toString() + "," + kv.getValue().toString() + ")";
        receiver.output(s);
    }
}
