package dev.yoichiro.test.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class PubsubToDatastore {

    public void execute(String[] args) {
        PubsubToDatastoreOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(PubsubToDatastoreOptions.class);
        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("read from Pubsub 1", PubsubIO.readStrings().fromTopic(options.getFromTopic()))
                .apply("transform Pubsub data", new TransformData(options))
                .apply("write to Datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

        pipeline.apply("read from Pubsub 2", PubsubIO.readStrings().fromTopic(options.getFromTopic()))
                .apply("simple output data 2", ParDo.of(new SimpleOutputDataFn<String>()));

        pipeline.apply("read from Pubsub 3", PubsubIO.readStrings().fromTopic(options.getFromTopic()))
//                .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
//                        .withAllowedLateness(Duration.standardMinutes(5))
//                        .discardingFiredPanes())
                .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(ParDo.of(new ExtractWordCountFn()))
                .apply(Sum.integersPerKey())
                .apply("simple output data 3", ParDo.of(new SimpleOutputDataFn<>()));

        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        PubsubToDatastore pubsubToDatastore = new PubsubToDatastore();
        pubsubToDatastore.execute(args);
    }

}
