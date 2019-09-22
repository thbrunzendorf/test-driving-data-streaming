package de.thbrunzendorf.beam.weather;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class MeasurementSource {

    private static final int MIN_TEMPERATURE = -30;
    private static final int MAX_TEMPERATURE = 50;
    private static final List<String> LOCATIONS = Arrays.asList(
            "London", "Paris", "Berlin", "Rome", "Stockholm", "Helsinki",
            "Copenhagen", "Oslo", "Reykjavik", "Vienna", "Madrid", "Athens",
            "Dublin", "Prague", "Warsaw", "Moscow", "Brussels", "Amsterdam");

    public static Measurement generateMeasurement() {
        int locationIndex = ThreadLocalRandom.current().nextInt(0, LOCATIONS.size());
        String location = LOCATIONS.get(locationIndex);
        Integer temperature = ThreadLocalRandom.current().nextInt(MIN_TEMPERATURE, MAX_TEMPERATURE + 1);
        return new Measurement(location, temperature);
    }

    public static List<Measurement> generateMeasurements(int minNumberMeasurements, int maxNumberMeasurements) {
        int numberOfMeasurements = ThreadLocalRandom.current().nextInt(minNumberMeasurements, maxNumberMeasurements + 1);
        List<Measurement> measurements = new ArrayList<>();
        for (int i = 1; i < numberOfMeasurements; i++) {
            measurements.add(generateMeasurement());
        }
        return measurements;
    }

    public static void generateMeasurementsFile(int interval, int duration, String outputPath, int minNumberMeasurements, int maxNumberMeasurements) {
        String prefix = "/weather-input-";
        long currentTime = 0;
        try {
            while (duration == -1 || currentTime < duration) {
                List<Measurement> measurements = generateMeasurements(minNumberMeasurements, maxNumberMeasurements);
                Writer write = new FileWriter(outputPath + prefix + UUID.randomUUID() + ".txt");
                for (Measurement measurement : measurements) {
                    String csv = measurement.location + "," + measurement.temperature + "\n";
                    write.write(csv);
                }
                write.flush();
                write.close();
                currentTime += interval;
                Thread.sleep(interval);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String outputPath = new File("build/resources/test/weather/").getPath();
        int minNumberMeasurements = 50;
        int maxNumberMeasurements = 100;
        int interval = 500;
        int duration = 30000;
        generateMeasurementsFile(interval, duration, outputPath, minNumberMeasurements, maxNumberMeasurements);
    }
}
