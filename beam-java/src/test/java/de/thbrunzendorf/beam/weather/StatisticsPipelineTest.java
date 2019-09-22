package de.thbrunzendorf.beam.weather;

import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static de.thbrunzendorf.beam.weather.StatisticsPipeline.*;

public class StatisticsPipelineTest {

    private Instant baseTime = new Instant(0);

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Before
    public void setUp() throws Exception {
        CoderRegistry coderRegistry = p.getCoderRegistry();
        coderRegistry.registerCoderForClass(Measurement.class, MEASUREMENT_CODER);
    }

    @Test
    public void providesWeatherStatisticsFromEmptyList() {
        List<Measurement> input = new ArrayList();
        PCollection<Measurement> pInput = p.apply(Create.of(input).withCoder(MEASUREMENT_CODER));

        PCollection<KV<String, Statistics>> pOutput = new StatisticsPipeline().provideWeatherStatistics(pInput);

        PAssert.that(pOutput).empty();

        p.run();
    }

    @Test
    public void providesWeatherStatisticsFromListWithOneMeasurement() {
        List<Measurement> input = new ArrayList();
        input.add(new Measurement("Berlin", 25));
        PCollection<Measurement> pInput = p.apply(Create.of(input));

        PCollection<KV<String, Statistics>> pOutput = new StatisticsPipeline().provideWeatherStatistics(pInput);

        PAssert.that(pOutput).containsInAnyOrder(KV.of("Berlin", new Statistics(1L, 25)));

        p.run();
    }

    @Test
    public void providesWeatherStatisticsFromListWithMultipleMeasurements() {
        List<Measurement> input = new ArrayList();
        input.add(new Measurement("London", 15));
        input.add(new Measurement("Berlin", 25));
        input.add(new Measurement("London", 17));
        PCollection<Measurement> pInput = p.apply(Create.of(input));

        PCollection<KV<String, Statistics>> pOutput = new StatisticsPipeline().provideWeatherStatistics(pInput);

        PAssert.that(pOutput).containsInAnyOrder(
                KV.of("Berlin", new Statistics(1L, 25)),
                KV.of("London", new Statistics(2L, 16)));

        p.run();
    }

    @Test
    public void providesWeatherStatisticsFromUnboundedStreamOfMeasurementsBasic() {

        TestStream<Measurement> createEvents = TestStream.create(MEASUREMENT_CODER)
                .addElements(new Measurement("London", 15),
                        new Measurement("Berlin", 25),
                        new Measurement("London", 17))
                .advanceWatermarkToInfinity();

        PCollection<Measurement> pInput = p.apply(createEvents);
        PCollection<KV<String, Statistics>> pOutput = new StatisticsPipeline().provideWeatherStatistics(pInput);

        PAssert.that(pOutput).containsInAnyOrder(
                KV.of("Berlin", new Statistics(1L, 25)),
                KV.of("London", new Statistics(2L, 16)));

        p.run();

    }

    @Test
    public void providesWeatherStatisticsFromUnboundedStreamOfMeasurementsDifferentWindows() {

        TestStream<Measurement> createEvents = TestStream.create(MEASUREMENT_CODER)
                .advanceWatermarkTo(baseTime)
                .addElements(new Measurement("London", 15),
                        new Measurement("Berlin", 25))
                .advanceWatermarkTo(baseTime.plus(WEATHER_WINDOW_DURATION)
                        .plus(Duration.standardSeconds(1)))
                .addElements(new Measurement("London", 17))
                .advanceWatermarkToInfinity();

        PCollection<Measurement> pInput = p.apply(createEvents);
        PCollection<KV<String, Statistics>> pOutput = new StatisticsPipeline().provideWeatherStatistics(pInput);

        PAssert.that(pOutput)
                .containsInAnyOrder(
                        KV.of("Berlin", new Statistics(1L, 25)),
                        KV.of("London", new Statistics(1L, 15)),
                        KV.of("London", new Statistics(1L, 17)));
        BoundedWindow window1 = new IntervalWindow(baseTime, WEATHER_WINDOW_DURATION);
        PAssert.that(pOutput)
                .inWindow(window1)
                .containsInAnyOrder(
                        KV.of("Berlin", new Statistics(1L, 25)),
                        KV.of("London", new Statistics(1L, 15)));
        BoundedWindow window2 = new IntervalWindow(baseTime.plus(WEATHER_WINDOW_DURATION), WEATHER_WINDOW_DURATION);
        PAssert.that(pOutput)
                .inWindow(window2)
                .containsInAnyOrder(
                        KV.of("London", new Statistics(1L, 17)));

        p.run();

    }

    // next: eventTime vs processingTime
    private TimestampedValue<Measurement> measurementEvent(
            String location, int temperature, Duration baseTimeOffset) {
        return TimestampedValue.of(new Measurement(location, temperature),
                baseTime.plus(baseTimeOffset));
    }

    @Test
    public void providesWeatherStatisticsFromTimestampedValues() {
        TestStream<Measurement> createEvents = TestStream.create(MEASUREMENT_CODER)
                .addElements(measurementEvent("London", 15, Duration.standardMinutes(1)),
                        measurementEvent("Berlin", 25, Duration.standardMinutes(2)),
                        measurementEvent("London", 17, WEATHER_WINDOW_DURATION.plus(Duration.standardMinutes(1))))
                .advanceWatermarkToInfinity();

        PCollection<Measurement> pInput = p.apply(createEvents);
        PCollection<KV<String, Statistics>> pOutput = new StatisticsPipeline().provideWeatherStatistics(pInput);

        PAssert.that(pOutput).containsInAnyOrder(
                KV.of("Berlin", new Statistics(1L, 25)),
                KV.of("London", new Statistics(1L, 15)),
                KV.of("London", new Statistics(1L, 17)));

        p.run();
    }

    @Test
    public void providesWeatherStatisticsFromTimestampedValuesWithinAllowedLateness() {
        TestStream<Measurement> createEvents = TestStream.create(MEASUREMENT_CODER)
                .addElements(measurementEvent("London", 15, Duration.standardSeconds(1)),
                        measurementEvent("Berlin", 25, Duration.standardSeconds(2)))
                .advanceWatermarkTo(baseTime.plus(WEATHER_WINDOW_DURATION)
                        .plus(Duration.standardSeconds(1)))
                .addElements(measurementEvent("London", 17, Duration.standardSeconds(3)))
                .advanceWatermarkToInfinity();

        PCollection<Measurement> pInput = p.apply(createEvents);
        PCollection<KV<String, Statistics>> pOutput = new StatisticsPipeline().provideWeatherStatistics(pInput);

        PAssert.that(pOutput).containsInAnyOrder(
                KV.of("Berlin", new Statistics(1L, 25)),
                KV.of("London", new Statistics(1L, 15)),
                KV.of("London", new Statistics(2L, 16)));

        BoundedWindow window1 = new IntervalWindow(baseTime, WEATHER_WINDOW_DURATION);
        PAssert.that(pOutput)
                .inWindow(window1)
                .containsInAnyOrder(
                        KV.of("Berlin", new Statistics(1L, 25)),
                        KV.of("London", new Statistics(1L, 15)),
                        KV.of("London", new Statistics(2L, 16)));
        BoundedWindow window2 = new IntervalWindow(baseTime.plus(WEATHER_WINDOW_DURATION), WEATHER_WINDOW_DURATION);
        PAssert.that(pOutput)
                .inWindow(window2)
                .empty();

        p.run();
    }

    @Test
    public void providesWeatherStatisticsFromTimestampedValuesOutsideOfAllowedLateness() {
        TestStream<Measurement> createEvents = TestStream.create(MEASUREMENT_CODER)
                .addElements(measurementEvent("London", 15, Duration.standardSeconds(1)),
                        measurementEvent("Berlin", 25, Duration.standardSeconds(2)))
                .advanceWatermarkTo(baseTime.plus(WEATHER_WINDOW_DURATION).plus(WEATHER_WINDOW_ALLOWED_LATENESS)
                        .plus(Duration.standardSeconds(1)))
                .addElements(measurementEvent("London", 17, Duration.standardSeconds(3)))
                .advanceWatermarkToInfinity();

        PCollection<Measurement> pInput = p.apply(createEvents);
        PCollection<KV<String, Statistics>> pOutput = new StatisticsPipeline().provideWeatherStatistics(pInput);

        PAssert.that(pOutput).containsInAnyOrder(
                KV.of("Berlin", new Statistics(1L, 25)),
                KV.of("London", new Statistics(1L, 15)));

        BoundedWindow window1 = new IntervalWindow(baseTime, WEATHER_WINDOW_DURATION);
        PAssert.that(pOutput)
                .inWindow(window1)
                .containsInAnyOrder(
                        KV.of("Berlin", new Statistics(1L, 25)),
                        KV.of("London", new Statistics(1L, 15)));
        BoundedWindow window2 = new IntervalWindow(baseTime.plus(WEATHER_WINDOW_DURATION), WEATHER_WINDOW_DURATION);
        PAssert.that(pOutput)
                .inWindow(window2)
                .empty();

        p.run();
    }

}
