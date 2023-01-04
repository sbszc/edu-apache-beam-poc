package com.eqfx.latam.poc.scenario;

import com.eqfx.latam.poc.csv.CSVRecordMap;
import com.eqfx.latam.poc.csv.CsvIO;
import com.eqfx.latam.poc.csv.CsvParsers;
import com.eqfx.latam.poc.csv.CsvTransformer;
import com.eqfx.latam.poc.model.SaleOrder;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;

import java.util.function.BiConsumer;

import static com.eqfx.latam.poc.csv.util.CsvConstants.DELIMITER_SCENARIO_2;
import static com.eqfx.latam.poc.csv.util.CsvConstants.HEADERS_SCENARIO_2;

@NoArgsConstructor
public class ScenarioTwoBiConsumer implements BiConsumer<Pipeline, SalesByQuarter.Options> {

    @Override
    public void accept(Pipeline pipeline, SalesByQuarter.Options options) {
        PCollection<CSVRecordMap> csvRecordMap = pipeline.apply("Reading from csv",
                CsvIO.read(options.getSourceFile())
                        .withCsvTransformer(
                                CsvTransformer.of()
                                        .withDelimiter(DELIMITER_SCENARIO_2)
                                        .withHeaders(HEADERS_SCENARIO_2)
                                        .build()
                        ).build()
        );
        PCollection<SaleOrder> csvMapped = csvRecordMap.apply("Parse to Sale", CsvParsers.saleOrders());
        PCollection<SalesByQuarter.Result> result = SalesByQuarter.apply(options.as(SalesByQuarter.Options.class), csvMapped);
        result.apply("Save to avro", AvroIO.write(SalesByQuarter.Result.class)
                .to(options.getTargetFile())
                .withSuffix(".avro"));
    }
}
