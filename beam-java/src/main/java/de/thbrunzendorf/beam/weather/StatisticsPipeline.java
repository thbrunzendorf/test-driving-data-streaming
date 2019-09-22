package de.thbrunzendorf.beam.weather;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import java.util.Objects;

public class StatisticsPipeline {

    public static final Coder<Measurement> MEASUREMENT_CODER = AvroCoder.of(Measurement.class);
    public static final Coder<Statistics> STATISTICS_CODER = AvroCoder.of(Statistics.class);
    public static final Coder<String> STRING_CODER = StringUtf8Coder.of();

    public static final Duration WEATHER_WINDOW_DURATION = Duration.standardSeconds(10);
    public static final Duration WEATHER_WINDOW_ALLOWED_LATENESS = Duration.standardSeconds(20);

    public static void main(String[] args) {

        // create pipeline
        Pipeline pipeline = Pipeline.create();

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForClass(Measurement.class, MEASUREMENT_CODER);

        PCollection<String> pipelineInput = pipeline
                .apply(TextIO
                        .read()
                        .from("build/resources/test/weather/weather-input-*.txt")
                        .watchForNewFiles(Duration.standardSeconds(1), Watch.Growth.<String>never()));

        PCollection<Measurement> measurements = pipelineInput
                .apply(MapElements.into(TypeDescriptor.of(Measurement.class)).via(StatisticsPipeline::parseStringAsMeasurement));

        // transform
        PCollection<KV<String, Statistics>> statistics = new StatisticsPipeline().provideWeatherStatistics(measurements);

        PCollection<String> pipelineOutput = statistics
                .apply(MapElements.into(TypeDescriptors.strings()).via(StatisticsPipeline::formatStatisticsAsString));

        // publish output
        pipelineOutput
                .apply(TextIO
                        .write()
                        .to("build/resources/test/weather/weather-output-")
                        .withSuffix(".txt")
                        .withWindowedWrites()
                        .withNumShards(2));

        // run pipeline for maximum duration
        pipeline.run().waitUntilFinish(Duration.standardSeconds(30));
    }

    private static String formatStatisticsAsString(KV<String, Statistics> kv) {
        String location = kv.getKey();
        Statistics statistics = kv.getValue();
        return location + "," + statistics.count + "," + statistics.averageTemperature;
    }

    private static Measurement parseStringAsMeasurement(String line) {
        String[] values = line.split(",");
        String location = values[0];
        Integer temperature = Integer.valueOf(values[1]);
        return new Measurement(location, temperature);
    }

    public PCollection<KV<String, Statistics>> provideWeatherStatistics(PCollection<Measurement> measurements) {

        // additional step necessary for unbounded streams
        PCollection<Measurement> windowedMeasurements = measurements.apply(Window.<Measurement>into(FixedWindows.of(WEATHER_WINDOW_DURATION))
                .withAllowedLateness(WEATHER_WINDOW_ALLOWED_LATENESS)
                .accumulatingFiredPanes()
        );

        //PCollection<KV<String, Measurement>> measurementsByLocation = measurements.apply(ParDo.of(new GroupMeasurementsByLocation()));
        //simplified with MapElements
        PCollection<KV<String, Measurement>> measurementsByLocation = windowedMeasurements.apply(MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(Measurement.class)))
                .via((Measurement measurement) -> KV.of(measurement.location, measurement)));
        measurementsByLocation.setCoder(KvCoder.of(STRING_CODER, MEASUREMENT_CODER));

        PCollection<KV<String, Statistics>> statisticsByLocation = measurementsByLocation.apply(Combine.<String, Measurement, Statistics>perKey(new StatisticsFn()));
        statisticsByLocation.setCoder(KvCoder.of(STRING_CODER, STATISTICS_CODER));

        return statisticsByLocation;
    }
}

class GroupMeasurementsByLocation extends DoFn<Measurement, KV<String, Measurement>> {
    @ProcessElement
    public void processElement(@Element Measurement measurement, OutputReceiver<KV<String, Measurement>> receiver) {
        receiver.output(KV.of(measurement.location, measurement));
    }
}

class StatisticsFn extends Combine.CombineFn<Measurement, StatisticsFn.Accum, Statistics> {
    public static class Accum {
        int sum = 0;
        int count = 0;
    }

    @Override
    public Accum createAccumulator() {
        return new Accum();
    }

    @Override
    public Accum addInput(Accum accum, Measurement input) {
        accum.sum += input.temperature;
        accum.count++;
        return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = createAccumulator();
        for (Accum accum : accums) {
            merged.sum += accum.sum;
            merged.count += accum.count;
        }
        return merged;
    }

    @Override
    public Statistics extractOutput(Accum accum) {
        Integer averageTemperature = accum.sum / accum.count;
        return new Statistics((long) accum.count, averageTemperature);
    }

    @Override
    public Coder<Accum> getAccumulatorCoder(CoderRegistry registry, Coder<Measurement> inputCoder) throws CannotProvideCoderException {
        return AvroCoder.of(Accum.class);
    }
}

class Measurement {
    public String location;
    public Integer temperature;

    private Measurement() {
        // needed by AvroCoder
    }

    public Measurement(String location, Integer temperature) {
        this.location = location;
        this.temperature = temperature;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Measurement that = (Measurement) o;
        return Objects.equals(location, that.location) &&
                Objects.equals(temperature, that.temperature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, temperature);
    }
}

class Statistics {
    public Long count;
    public Integer averageTemperature;

    private Statistics() {
        // needed by AvroCoder
    }

    public Statistics(Long count, Integer averageTemperature) {
        this.count = count;
        this.averageTemperature = averageTemperature;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Statistics that = (Statistics) o;
        return Objects.equals(count, that.count) &&
                Objects.equals(averageTemperature, that.averageTemperature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, averageTemperature);
    }

    @Override
    public String toString() {
        return "Statistics{" +
                "count=" + count +
                ", averageTemperature=" + averageTemperature +
                '}';
    }
}
