package dev.yoichiro.test.dataflow;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleOutputDataFn extends DoFn<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleOutputDataFn.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        String input = c.element();
        LOG.info("Input: " + input);
        c.output(input);
    }

}
