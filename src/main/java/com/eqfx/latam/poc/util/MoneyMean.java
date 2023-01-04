package com.eqfx.latam.poc.util;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.beam.sdk.transforms.Combine;
import org.joda.money.CurrencyUnit;
import org.joda.money.Money;

import java.io.Serializable;
import java.math.RoundingMode;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

public class MoneyMean extends Combine.CombineFn<Money, MoneyMean.MoneyComb, Money> {


    @Override
    public MoneyComb createAccumulator() {
        return new MoneyComb(0,Money.zero(CurrencyUnit.USD));
    }

    @Override
    public MoneyComb addInput(MoneyComb mutableAccumulator, Money input) {
        return requireNonNull(mutableAccumulator).plus(input);
    }

    @Override
    public MoneyComb mergeAccumulators(Iterable<MoneyComb> accumulators) {
        return StreamSupport.stream(accumulators.spliterator(), false)
                .reduce(createAccumulator(), MoneyComb::plus);
    }

    @Override
    public Money extractOutput(MoneyComb accumulator) {
        return requireNonNull(accumulator).value
                .dividedBy(accumulator.count, RoundingMode.DOWN);
    }

    @ToString
    @RequiredArgsConstructor
    static class MoneyComb implements Serializable {
        private final long count;
        private final Money value;
        MoneyComb plus(Money money){
            return new MoneyComb(count+1,value.plus(money));
        }
        MoneyComb plus(MoneyComb comb){
            return new MoneyComb(count+comb.count,value.plus(comb.value));
        }

    }
}