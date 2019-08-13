package dev.yoichiro.test.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

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
