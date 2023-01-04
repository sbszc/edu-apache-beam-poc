package com.eqfx.latam.poc.scenario;

import com.eqfx.latam.poc.ScenarioOptions;
import com.eqfx.latam.poc.model.Product;
import com.eqfx.latam.poc.util.MoneyMean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.money.CurrencyUnit;
import org.joda.money.Money;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

public interface ProductAvgPrice {
    static PCollection<Result> apply(Options options, PCollection<Product> products) {

        Money abovePrice = Money.of(CurrencyUnit.USD,options.getAbovePrice());

        return products.apply("Group product by id and name",ParDo.of(new GroupProductFn()))
                .apply("Compute Average", Combine.perKey(new MoneyMean()))
                .apply(String.format("Filter average price above %.2f", abovePrice.getAmount()),
                        Filter.by(kv -> requireNonNull(kv.getValue()).isGreaterThan(abovePrice)))
                .apply("Map result", ParDo.of(new MapResultFn()));
    }

    class GroupProductFn extends DoFn<Product,KV<KV<Integer,String>,Money>>{
        @ProcessElement
        public void processElement(@Element Product product,
                                   OutputReceiver<KV<KV<Integer,String>,Money>> outputReceiver){
            KV<Integer, String> key = KV.of(product.getId(), product.getName());
            Money value = product.getUnitPrice();
            outputReceiver.output(KV.of(key,value));
        }
    }
    class MapResultFn extends DoFn<KV<KV<Integer,String>,Money>,Result>{
        @ProcessElement
        public void processElement(@Element KV<KV<Integer,String>,Money> element,
                                   OutputReceiver<Result> outputReceiver){
            KV<Integer, String> key = requireNonNull(element.getKey());
            Money value = requireNonNull(element.getValue());
            Result result = new Result(key.getKey(), key.getValue(), value);
            outputReceiver.output(result);
        }
    }

    interface Options extends ScenarioOptions {
        @Description("Filter products above this price")
        @Validation.Required
        @Default.Double(1000.00)
        Double getAbovePrice();
        void setAbovePrice(Double value);
    }

    @Data
    @NoArgsConstructor
    @EqualsAndHashCode
    @AllArgsConstructor
    class Result implements Serializable {
        private Integer id;
        private String name;
        private Money avgPrice;
    }
}
