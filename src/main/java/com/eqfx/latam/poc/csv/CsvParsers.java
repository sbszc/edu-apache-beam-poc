package com.eqfx.latam.poc.csv;

import com.eqfx.latam.poc.model.Product;
import com.eqfx.latam.poc.model.SaleOrder;
import org.joda.money.CurrencyUnit;
import org.joda.money.Money;

import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class CsvParsers {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public static CsvParser<SaleOrder> saleOrders(){
        return CsvParser.of(SaleOrder.class).using(input -> {
            String category = input.get("ProductCategoryID");
            String subCategory = input.get("ProductSubcategoryID");
            LocalDate date = Optional.ofNullable(input.get("OrderDate"))
                    .filter(e->!e.isEmpty())
                    .map(dateValue->LocalDateTime.parse(dateValue, formatter))
                    .map(LocalDateTime::toLocalDate)
                    .orElse(null);
            Money unitPrice = Optional.ofNullable(input.get("UnitPrice"))
                    .filter(e->!e.isEmpty())
                    .map(e->e.replace(',','.'))
                    .map(Double::parseDouble)
                    .map(value->Money.of(CurrencyUnit.USD,value,RoundingMode.DOWN))
                    .orElse(Money.zero(CurrencyUnit.USD));
            Integer qty = Integer.valueOf(input.get("OrderQty"));
            return new SaleOrder(category,subCategory, date, unitPrice, qty);
        });
    }

    public static CsvParser<Product> products(){
        return CsvParser.of(Product.class).using(input -> {
            Integer id = Integer.parseInt(input.get("ProductID"));
            String name = input.get("ProductName");
            Money unitPrice = Money.of(CurrencyUnit.USD, Double.parseDouble(
                    input.get("UnitPrice").replace(',','.')), RoundingMode.HALF_UP);
            return new Product(id, name, unitPrice);
        });
    }
}
