package com.jacky.demoApp.model;

import java.math.BigDecimal;

public record MarketData(String symbol, BigDecimal price, long timestamp) implements Comparable<MarketData> {
    @Override
    public int compareTo(MarketData other) {
        return Long.compare(this.timestamp, other.timestamp);
    }

}
