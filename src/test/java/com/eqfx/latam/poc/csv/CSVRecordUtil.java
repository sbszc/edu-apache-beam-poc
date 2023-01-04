package com.eqfx.latam.poc.csv;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.StringReader;

import static com.eqfx.latam.poc.csv.util.CsvConstants.DELIMITER_SCENARIO_2;

public class CSVRecordUtil {
    private CSVRecordUtil() {
    }

    static CSVRecordMap mockRecord(String[] headers, String... data) throws IOException {
        String values = StringUtils.join(data, ";");
        try (CSVParser parser = CSVFormat.DEFAULT.builder().setHeader(headers).setDelimiter(DELIMITER_SCENARIO_2).build().parse(new StringReader(values))) {
            CSVRecord record = parser.iterator().next();
            return CSVRecordMap.valueOf(record);
        }
    }
}
