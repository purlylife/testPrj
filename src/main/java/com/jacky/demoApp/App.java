package com.jacky.demoApp;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.jacky.demoApp.model.MarketData;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        AtomicLong fakeClock = new AtomicLong(10);
        Map<String, MarketData> dataRepo = new HashMap<>();

        MarketDataProcessor processor = new MarketDataProcessor(fakeClock::get, dataRepo);
        processor.onMessage(new MarketData("AAPL", BigDecimal.valueOf(150.00), 10));
        fakeClock.addAndGet(500);
        processor.onMessage(new MarketData("GOOGL", BigDecimal.valueOf(2800.00), 500));
        fakeClock.addAndGet(600);
        processor.onMessage(new MarketData("MSFT", BigDecimal.valueOf(300.00), 1100));
        System.out.println("Data Repo: " + dataRepo);
    }
}
