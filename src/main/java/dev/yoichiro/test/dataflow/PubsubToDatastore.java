package dev.yoichiro.test.dataflow;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

@SuppressWarnings("serial")
public class PubsubToDatastore {

    private static String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private static String TIME_ZONE = "Asia/Tokyo";
    private static DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern(TIME_FORMAT);

    static class AddDatetimeFn extends DoFn<String, Entity> {

        private String toNamespace;
        private String toKind;
        private String toNamePrefix;

        public AddDatetimeFn(String toNamespace, String toKind, String toNamePrefix) {
            this.toNamespace = toNamespace;
            this.toKind = toKind;
            this.toNamePrefix = toNamePrefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] columns = c.element().split(",");

            Entity.Builder entityBuilder = Entity.newBuilder();

            Integer jobId = Integer.parseInt(columns[0].trim());
            Integer rowId = Integer.parseInt(columns[1].trim());
            Key.Builder keyBuilder = makeKey(toKind, String.format("%s-%04d-%04d", toNamePrefix, jobId, rowId));
            if (toNamespace != null) {
                keyBuilder.getPartitionIdBuilder().setNamespaceId(toNamespace);
            }
            Integer wordNum = columns[2].trim().split(" ").length;
            String importDtStr = columns[3].trim();
            ZonedDateTime importDt = LocalDateTime.parse(importDtStr, dtFormatter).atZone(ZoneId.of(TIME_ZONE));

            ZonedDateTime updateDt = ZonedDateTime.now(ZoneId.of(TIME_ZONE));

            entityBuilder.setKey(keyBuilder.build());
            entityBuilder.putProperties("rowId", makeValue(rowId).build());
            entityBuilder.putProperties("sentence", makeValue(columns[2].trim()).build());
            entityBuilder.putProperties("wordNum", makeValue(wordNum).build());
            entityBuilder.putProperties("importTimestamp", makeValue(dtFormatter.format(importDt)).build());
            entityBuilder.putProperties("updateTimestamp", makeValue(dtFormatter.format(updateDt)).build());

            c.output(entityBuilder.build());
        }

    }

    public class TransformData extends PTransform<PCollection<String>, PCollection<Entity>> {

        private PubsubToDatastoreOptions options;

        public TransformData(PubsubToDatastoreOptions options) {
            this.options = options;
        }

        @Override
        public PCollection<Entity> expand(PCollection<String> line) {
            PCollection<Entity> rows = line.apply(ParDo.of(
                    new AddDatetimeFn(options.getToNamespace(), options.getToKind(), options.getToNamePrefix())
            ));
            return rows;
        }
    }

    public interface PubsubToDatastoreOptions extends DataflowPipelineOptions {

        String getFromTopic();
        void setFromTopic(String fromTopic);

        String getToNamespace();
        void setToNamespace(String toNamespace);

        String getToKind();
        void setToKind(String toKind);

        String getToNamePrefix();
        void setToNamePrefix(String toNamePrefix);

    }

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
