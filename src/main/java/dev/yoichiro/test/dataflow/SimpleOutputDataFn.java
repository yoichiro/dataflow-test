package dev.yoichiro.test.dataflow;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class SimpleOutputDataFn<T> extends DoFn<T, String> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleOutputDataFn.class);

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
        T input = c.element();
        LOG.info("Window: " + window.toString());
        LOG.info("Window.maxTimestamp: " + window.maxTimestamp().toString());
        LOG.info("Input: " + input.toString());
        c.output(input.toString());
    }

}
