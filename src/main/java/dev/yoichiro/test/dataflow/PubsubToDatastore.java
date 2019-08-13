package dev.yoichiro.test.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PubsubToDatastore {

    public void execute(String[] args) {
        PubsubToDatastoreOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(PubsubToDatastoreOptions.class);
        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("read from Pubsub", PubsubIO.readStrings().fromTopic(options.getFromTopic()))
                .apply("transform Pubsub data", new TransformData(options))
                .apply("write to Datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        PubsubToDatastore pubsubToDatastore = new PubsubToDatastore();
        pubsubToDatastore.execute(args);
    }

}
