package dev.yoichiro.test.dataflow;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

@SuppressWarnings("serial")
public class ExtractUserIdFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        String line = c.element();
        String[] columns = line.trim().split(",");
        String userId = columns[4].trim();
        c.output(userId);
    }

}
