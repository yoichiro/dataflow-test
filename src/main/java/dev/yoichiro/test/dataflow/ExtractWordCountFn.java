package dev.yoichiro.test.dataflow;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ExtractWordCountFn extends DoFn<String, KV<String, Integer>> {

    private static String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private static String TIME_ZONE = "Asia/Tokyo";
    private static DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern(TIME_FORMAT);

    @ProcessElement
    public void processElement(ProcessContext c) {
        String line = c.element();
        String[] columns = line.split(",");

        Integer wordCount = columns[2].trim().split(" ").length;

//        String importDtStr = columns[3].trim();
//        ZonedDateTime importDt = LocalDateTime.parse(importDtStr, dtFormatter).atZone(ZoneId.of(TIME_ZONE));

//        c.outputWithTimestamp(KV.of("wordcount", wordCount), Instant.ofEpochSecond(importDt.toEpochSecond()));
        c.output(KV.of("wordcount", wordCount));
    }

}
