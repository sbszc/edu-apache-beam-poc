package com.eqfx.latam.poc.csv;

import com.eqfx.latam.poc.model.SaleOrder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.joda.money.CurrencyUnit;
import org.joda.money.Money;
import org.junit.Rule;
import org.junit.Test;

import java.time.LocalDate;

public class CsvParsersTest {
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void product() throws Exception {
        String[] headers=new String[]{"ProductCategoryID", "ProductSubcategoryID", "SellEndDate","UnitPrice","OrderQty"};
        Create.Values<CSVRecordMap> lines = Create.of(
                CSVRecordUtil.mockRecord(headers,"1","1","2014-01-01 00:00:00.000","50","1"),
                CSVRecordUtil.mockRecord(headers,"2","2","2013-01-01 00:00:00.000","50","1"),
                CSVRecordUtil.mockRecord(headers,"2","2","2013-01-01 00:00:00.000","2024,994","1"),
                CSVRecordUtil.mockRecord(headers,"2","2","2013-01-01 00:00:00.000",null,"1"),
                CSVRecordUtil.mockRecord(headers,"2","2",null,"50","1")
        );
        PCollection<CSVRecordMap> linesPColl = testPipeline.apply(lines);
        CsvParser<SaleOrder> transformer = CsvParsers.saleOrders();

        PCollection<SaleOrder> asPojo = linesPColl.apply(transformer);

        PAssert.that(asPojo).containsInAnyOrder(
                new SaleOrder("1", "1", LocalDate.of(2014,1,1), Money.of(CurrencyUnit.USD, 50),1),
                new SaleOrder("2", "2", LocalDate.of(2013,1,1), Money.of(CurrencyUnit.USD, 50),1),
                new SaleOrder("2", "2", LocalDate.of(2013,1,1), Money.of(CurrencyUnit.USD, 2024.99),1),
                new SaleOrder("2", "2", LocalDate.of(2013,1,1), Money.zero(CurrencyUnit.USD),1),
                new SaleOrder("2", "2", null, Money.of(CurrencyUnit.USD, 50),1)
        );
        testPipeline.run().waitUntilFinish();
    }
}
