package com.eqfx.latam.poc.csv;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

public class CsvParserTest {
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void parse() throws IOException {
        String[] headers=new String[]{"id","name"};
        Create.Values<CSVRecordMap> lines = Create.of(
                CSVRecordUtil.mockRecord(headers,"1","model one"),
                CSVRecordUtil.mockRecord(headers,"2","model two")
        );
        PCollection<CSVRecordMap> linesPColl = testPipeline.apply(lines);

        SerializableFunction<CSVRecordMap,Model> mapper= record ->
                new Model(record.get(0),record.get("name"));
        CsvParser<Model> transformer = CsvParser.of(Model.class).using(mapper);

        PCollection<Model> asPojo = linesPColl.apply(transformer);

        PAssert.that(asPojo).containsInAnyOrder(
                new Model("1","model one"),
                new Model("2","model two")
        );
        testPipeline.run().waitUntilFinish();
    }
    @AllArgsConstructor
    @EqualsAndHashCode
    static class Model implements Serializable{
        String id;
        String name;
    }

}