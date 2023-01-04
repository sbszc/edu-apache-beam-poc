package com.eqfx.latam.poc.csv;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;


class CsvParser<T> extends PTransform<PCollection<CSVRecordMap>, PCollection<T>> {


    static <T> Builder<T> of(Class<T> tClass) {
        return new Builder<>(tClass);
    }

    private final SerializableFunction<CSVRecordMap, T> mapper;
    private final Class<T> clazz;

    private CsvParser(Class<T> clazz, SerializableFunction<CSVRecordMap, T> mapper) {
        this.mapper = mapper;
        this.clazz = clazz;
    }

    @Override
    public PCollection<T> expand(PCollection<CSVRecordMap> stingLine) {
        return stingLine.apply(MapElements.into(TypeDescriptor.of(clazz)).via(mapper));
    }

    static class Builder<T> {
        private final Class<T> clazz;
        private Builder(Class<T> clazz) { this.clazz = clazz; }
        public CsvParser<T> using(SerializableFunction<CSVRecordMap, T> mapper) {
            return new CsvParser<>(clazz, mapper);
        }
    }
}


