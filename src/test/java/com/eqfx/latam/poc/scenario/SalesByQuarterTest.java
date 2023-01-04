package com.eqfx.latam.poc.scenario;

import com.eqfx.latam.poc.model.SaleOrder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.joda.money.Money;
import org.junit.Rule;
import org.junit.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static com.eqfx.latam.poc.scenario.SalesByQuarter.Options;
import static com.eqfx.latam.poc.scenario.SalesByQuarter.Result;
import static org.joda.money.CurrencyUnit.USD;

public class SalesByQuarterTest {
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/M/yyyy");

    static final List<SaleOrder> sales = List.of(
            new SaleOrder("Category", "SubCategory", null, Money.of(USD, 50.00), 2),
            new SaleOrder("Category", "SubCategory", parseDate("15/01/2013"), Money.of(USD, 50.00), 2),
            new SaleOrder("Category", "SubCategory", parseDate("22/02/2013"), Money.of(USD, 50.99), 1),
            new SaleOrder("Category", "SubCategory", parseDate("11/04/2013"), Money.of(USD, 100.00), 5),
            new SaleOrder("Category", "SubCategory", parseDate("20/08/2014"), Money.of(USD, 75.00), 1),
            new SaleOrder("Category", "SubCategory", parseDate("07/12/2014"), Money.of(USD, 80.99), 3),
            new SaleOrder("Category", "SubCategory", parseDate("07/12/2014"), Money.of(USD, 22.99), 1),
            new SaleOrder("Category", "SubCategory", parseDate("15/01/2015"), Money.of(USD, 50.00), 2),
            new SaleOrder("Category", "SubCategory", parseDate("22/02/2015"), Money.of(USD, 50.99), 1),
            new SaleOrder("Category", "SubCategory", parseDate("11/04/2015"), Money.of(USD, 100.00), 5)
    );
    static final List<Result> expected = List.of(
            new Result("Category", "SubCategory", 1, 2013, Money.of(USD, 150.99)),
            new Result("Category", "SubCategory", 2, 2013, Money.of(USD, 500.00)),
            new Result("Category", "SubCategory", 3, 2014, Money.of(USD, 75.00)),
            new Result("Category", "SubCategory", 4, 2014, Money.of(USD, 265.96))
    );

    static LocalDate parseDate(String date) {
        return LocalDate.parse(date, dateTimeFormatter);
    }

    @Test
    public void apply() {
        Options testOptions =
                TestPipeline.testingPipelineOptions().as(Options.class);
        testOptions.setYears(List.of(2013, 2014));
        PCollection<SaleOrder> saleOrderPCollection = testPipeline.apply("Create values From Sales", Create.of(sales));
        PCollection<Result> resultPCollection = SalesByQuarter.apply(testOptions, saleOrderPCollection);
        PAssert.that(resultPCollection).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();

    }

}