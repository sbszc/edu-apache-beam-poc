package com.eqfx.latam.poc.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.joda.money.Money;

import java.io.Serializable;
@Getter
@EqualsAndHashCode
@RequiredArgsConstructor
public class Product implements Serializable {
    private final Integer id;
    private final String name;
    private final Money unitPrice;
}
