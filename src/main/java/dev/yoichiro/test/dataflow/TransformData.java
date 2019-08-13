package dev.yoichiro.test.dataflow;

import com.google.datastore.v1.Entity;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

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
