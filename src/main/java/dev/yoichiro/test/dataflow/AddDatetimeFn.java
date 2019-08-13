package dev.yoichiro.test.dataflow;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import org.apache.beam.sdk.transforms.DoFn;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

public class AddDatetimeFn extends DoFn<String, Entity> {

    private static String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private static String TIME_ZONE = "Asia/Tokyo";
    private static DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern(TIME_FORMAT);

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