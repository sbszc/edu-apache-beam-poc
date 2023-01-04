package com.eqfx.latam.poc.csv;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.util.Objects;

public interface CsvIO {
    static Read.Builder read(String file){
        return new Read.Builder(file);
    }
    class Read extends PTransform<PBegin, PCollection<CSVRecordMap>> {
        private final String file;
        private final CsvTransformer transformer;

        private Read(String file,CsvTransformer transformer) {
            this.file = Objects.requireNonNull(file);
            this.transformer=transformer;
        }

        @Override
        public PCollection<CSVRecordMap> expand(PBegin input) {
            return input
                    .apply(FileIO.match().filepattern(file))
                    .apply(FileIO.readMatches())
                    .apply(transformer);
        }

        public static class Builder {
            private final String file;
            private CsvTransformer transformer;

            private Builder(String file) { this.file = file; }


            public Builder withCsvTransformer(CsvTransformer transformer) {
                this.transformer = transformer;
                return this;
            }

            public Read build() {
                return new Read(file, transformer);
            }
        }
    }
}
