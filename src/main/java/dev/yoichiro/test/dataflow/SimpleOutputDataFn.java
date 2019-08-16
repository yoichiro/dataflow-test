package dev.yoichiro.test.dataflow;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class SimpleOutputDataFn<T> extends DoFn<T, String> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleOutputDataFn.class);

    private static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS ZZZ");

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
        T input = c.element();
        LOG.info("Window: " + window.toString());
        LOG.info("Window.maxTimestamp: " + formatter.print(window.maxTimestamp()));
        LOG.info("Input: " + input.toString());
        c.output(input.toString());
    }

}
