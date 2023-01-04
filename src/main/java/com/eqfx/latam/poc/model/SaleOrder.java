package com.eqfx.latam.poc.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.joda.money.Money;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Optional;

@Getter
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class SaleOrder implements Serializable {
    private final String category;
    private final String subcategory;
    private final LocalDate date;
    private final Money unitPrice;
    private final Integer qty;

    public Optional<LocalDate> getDate() {
        return Optional.ofNullable(date);
    }
}
