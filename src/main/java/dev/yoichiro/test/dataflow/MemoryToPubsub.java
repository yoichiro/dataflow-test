package dev.yoichiro.test.dataflow;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryToPubsub {

    private static String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private static String TIME_ZONE = "Asia/Tokyo";
    private static DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern(TIME_FORMAT);

    public class PubsubJob {

        private String toTopic;
        private Integer jobId;
        private Integer rowNum;
        private String userId;

        public PubsubJob(String toTopic, int jobId, int rowNum, String userId) {
            this.toTopic = toTopic;
            this.jobId = jobId;
            this.rowNum = rowNum;
            this.userId = userId;
        }

        public void publishData() throws IOException {
            Pubsub pubsub = PortableConfiguration.createPubsubClient();
            for (int i = 1; i <= rowNum; i++) {
                String rowId = String.valueOf(i);
                // "1,1,JobId: 1  rowId: 1 no test desu,2018-05-01 14:02:38.420"
                ZonedDateTime now = ZonedDateTime.now(ZoneId.of(TIME_ZONE));
                String message = jobId + "," + rowId + ",JobId: " + jobId + " rowId: " + rowId
                        + " no test desu," + dtFormatter.format(now) + "," + userId;
                PubsubMessage pubsubMessage = new PubsubMessage();
                pubsubMessage.encodeData(message.getBytes("UTF-8"));
                Map<String, String> attributes = new HashMap<>();
                attributes.put("eventTimestamp", String.valueOf(now.toInstant().toEpochMilli()));
                System.out.println(attributes);
                pubsubMessage.setAttributes(attributes);
                List<PubsubMessage> messages = ImmutableList.of(pubsubMessage);
                PublishRequest publishRequest = new PublishRequest().setMessages(messages);

                pubsub.projects().topics()
                        .publish(toTopic, publishRequest)
                        .execute();

                System.out.println("Sent: " + message);
            }
        }

        public void doJob() {
            try {
                publishData();
            } catch(IOException e) {
                e.printStackTrace();
            }
        }

    }

    public interface MemoryToPubsubOptions extends PipelineOptions {

        String getToTopic();
        void setToTopic(String toTopic);

        @Default.Integer(1)
        Integer getJobNum();
        void setJobNum(Integer jobNum);

        @Default.Integer(1)
        Integer getRowNum();
        void setRowNum(Integer rowNum);

        @Default.String("")
        String getUserId();
        void setUserId(String userId);

    }

    public void execute(String[] args) {
        MemoryToPubsubOptions options = PipelineOptionsFactory.fromArgs(args).as(MemoryToPubsubOptions.class);

        String toTopic = options.getToTopic();
        int jobNum = options.getJobNum();
        int rowNum = options.getRowNum();
        String userId = options.getUserId();

        List<PubsubJob> jobs = new ArrayList<>();
        for (int i = 1; i <= jobNum; i++) {
            jobs.add(new PubsubJob(toTopic, i, rowNum, userId));
        }
        jobs.parallelStream().forEach(x -> {
            x.doJob();
        });
    }

    public static void main(String[] args) {
        MemoryToPubsub memoryToPubsub = new MemoryToPubsub();
        memoryToPubsub.execute(args);
    }

}
