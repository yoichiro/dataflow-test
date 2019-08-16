package dev.yoichiro.test.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

public class PubsubToDatastore {

    public void execute(String[] args) {
        PubsubToDatastoreOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(PubsubToDatastoreOptions.class);
        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);

        // Pubsub -> Datastore
        pipeline.apply("read from Pubsub 1", PubsubIO.readStrings().fromTopic(options.getFromTopic()))
                .apply("transform Pubsub data", new TransformData(options))
                .apply("write to Datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

        // Pubsub -> Stdout
        pipeline.apply("read from Pubsub 2", PubsubIO.readStrings().fromTopic(options.getFromTopic()))
                .apply("simple output data 2", ParDo.of(new SimpleOutputDataFn<String>()));

        // Pubsub -> Window -> Sum -> Stdout
        pipeline.apply("read from Pubsub 3", PubsubIO.readStrings().fromTopic(options.getFromTopic())
                        .withTimestampAttribute("eventTimestamp"))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                        .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                        .withAllowedLateness(Duration.standardMinutes(10))
                        .accumulatingFiredPanes())
                .apply(ParDo.of(new ExtractWordCountFn()))
                .apply(Sum.integersPerKey())
                .apply("simple output data 3", ParDo.of(new SimpleOutputDataFn<>()));

        // Pubsub -> Window -> Distinct -> Count -> Stdout
        pipeline.apply("read from Pubsub 4", PubsubIO.readStrings().fromTopic(options.getFromTopic())
                        .withTimestampAttribute("eventTimestamp"))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                        .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                        .withAllowedLateness(Duration.standardMinutes(10))
                        .accumulatingFiredPanes())
                .apply(ParDo.of(new ExtractUserIdFn()))
                .apply(Distinct.create())
                .apply(Combine.globally(Count.<String>combineFn()).withoutDefaults())
                .apply("simple output data 4", ParDo.of(new SimpleOutputDataFn<>()));

        // Pubsub -> Window -> Sum -> Stdout
        pipeline.apply("read from Pubsub 5", PubsubIO.readStrings().fromTopic(options.getFromTopic())
                        .withTimestampAttribute("eventTimestamp"))
                .apply(Window.<String>into(
                            FixedWindows.of(Duration.standardDays(1))
                                    .withOffset(Duration.standardHours(15)))
                        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                        .withAllowedLateness(Duration.standardMinutes(10))
                        .accumulatingFiredPanes())
                .apply(ParDo.of(new ExtractWordCountFn()))
                .apply(Sum.integersPerKey())
                .apply("simple output data 3", ParDo.of(new SimpleOutputDataFn<>()));

        // Pubsub -> Window -> Sum -> Stdout
        pipeline.apply("read from Pubsub 6", PubsubIO.readStrings().fromTopic(options.getFromTopic())
                        .withTimestampAttribute("eventTimestamp"))
                .apply(Window.<String>into(
                        CalendarWindows.days(1).withTimeZone(DateTimeZone.forID("Asia/Tokyo")))
                        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                        .withAllowedLateness(Duration.standardMinutes(10))
                        .accumulatingFiredPanes())
                .apply(ParDo.of(new ExtractWordCountFn()))
                .apply(Sum.integersPerKey())
                .apply("simple output data 6", ParDo.of(new SimpleOutputDataFn<>()));

        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        PubsubToDatastore pubsubToDatastore = new PubsubToDatastore();
        pubsubToDatastore.execute(args);
    }

}
