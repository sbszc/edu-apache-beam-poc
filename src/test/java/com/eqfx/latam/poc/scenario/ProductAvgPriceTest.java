package com.eqfx.latam.poc.scenario;

import com.eqfx.latam.poc.model.Product;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.joda.money.CurrencyUnit;
import org.joda.money.Money;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static com.eqfx.latam.poc.scenario.ProductAvgPrice.Options;
import static com.eqfx.latam.poc.scenario.ProductAvgPrice.Result;

public class ProductAvgPriceTest {
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void apply(){
        Options testOptions =
                TestPipeline.testingPipelineOptions().as(Options.class);
        testOptions.setAbovePrice(100.00);

        PCollection<Product> products = testPipeline.apply("Create values", Create.of(
                new Product(1, "Product 1", Money.of(CurrencyUnit.USD,250.00)),
                new Product(1, "Product 1", Money.of(CurrencyUnit.USD,100.00)),
                new Product(1, "Product 1", Money.of(CurrencyUnit.USD,500.00)),
                new Product(2, "Product 2", Money.of(CurrencyUnit.USD,60)),
                new Product(2, "Product 2", Money.of(CurrencyUnit.USD,100.00)),
                new Product(2, "Product 2", Money.of(CurrencyUnit.USD,90.00))
        ));
        PCollection<Result> resultPCollection = ProductAvgPrice.apply(testOptions, products);

        PAssert.that(resultPCollection).containsInAnyOrder(List.of(
                new Result(1,"Product 1",Money.of(CurrencyUnit.USD,283.33))
        ));
        testPipeline.run();




    }

}