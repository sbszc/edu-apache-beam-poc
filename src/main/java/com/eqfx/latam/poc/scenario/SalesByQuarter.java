package com.eqfx.latam.poc.scenario;

import com.eqfx.latam.poc.ScenarioOptions;
import com.eqfx.latam.poc.model.SaleOrder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.money.Money;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.util.List;

import static java.util.Objects.requireNonNull;

public interface SalesByQuarter {

    static PCollection<Result> apply(Options options, PCollection<SaleOrder> sales){
        List<Integer> years = options.getYears();
        return sales.apply("Filter years", Filter.by(sale-> sale.getDate().map(LocalDate::getYear).filter(years::contains).isPresent()))
                .apply("Group by quarter and year", ParDo.of(new GroupSalesFn()))
                .apply("Sum sales", Combine.perKey(Money::plus))
                .apply("Map result",ParDo.of(new MapResultFn()));
    }

    class GroupSalesFn extends DoFn<SaleOrder,
            KV<KV<KV<Integer,Integer>,KV<String,String>>,Money>> {

        @ProcessElement
        public void processElement(@Element SaleOrder order,
                                   OutputReceiver<KV<KV<KV<Integer,Integer>,KV<String,String>>,Money>> outputReceiver){

            order.getDate().ifPresent(date -> {

                int year = date.getYear();
                int quarter = date.get(IsoFields.QUARTER_OF_YEAR);

                KV<Integer, Integer> yearQuarter = KV.of(year, quarter);
                KV<String, String> category = KV.of(order.getCategory(), order.getSubcategory());
                Money sale = order.getUnitPrice().multipliedBy(order.getQty());
                outputReceiver.output(KV.of(
                        KV.of(yearQuarter, category),
                        sale
                ));
            });
        }
    }
    class MapResultFn extends DoFn<KV<KV<KV<Integer,Integer>,KV<String,String>>,Money>
            ,Result>{
        @ProcessElement
        public void processElement(@Element KV<KV<KV<Integer,Integer>,KV<String,String>>,Money> element,
                                   OutputReceiver<Result> outputReceiver){

            KV<KV<Integer, Integer>, KV<String, String>> key = requireNonNull(element.getKey());
            KV<Integer, Integer> yearQuarter = requireNonNull(key.getKey());
            KV<String, String> category = requireNonNull(key.getValue());
            Money value = element.getValue();

            Result result = new Result(category.getKey(), category.getValue(), yearQuarter.getValue(), yearQuarter.getKey(), value);
            outputReceiver.output(result);
        }
    }

    @Getter
    @ToString
    @RequiredArgsConstructor
    @EqualsAndHashCode
    class Result implements Serializable {
        private final String category;
        private final String subcategory;
        private final Integer quarter;
        private final Integer year;
        private final Money sales;
    }

    interface Options extends ScenarioOptions {
        @Description("Filtered years")
        @Validation.Required
        List<Integer> getYears();
        void setYears(List<Integer> years);
    }
}
