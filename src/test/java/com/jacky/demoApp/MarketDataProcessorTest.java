package com.jacky.demoApp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.jacky.demoApp.model.MarketData;

public class MarketDataProcessorTest {
    private MarketDataProcessor processor;
    private Map<String, MarketData> dataRepo = new HashMap<>();
    private final AtomicLong fakeClock = new AtomicLong(0);

    @BeforeEach
    public void setUp() {
        processor = new MarketDataProcessor(fakeClock::get, dataRepo);
        fakeClock.set(10);
    }

    @Test
    void testInvalidData() {
        try {
            processor.onMessage(new MarketData("", BigDecimal.valueOf(100), fakeClock.get()));
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid symbol", e.getMessage());
        }

        try {
            processor.onMessage(new MarketData("AAPL", BigDecimal.valueOf(-50), fakeClock.get()));
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid price", e.getMessage());
        }
    }

    @Test
    void testWindowLimit() {
        for (int i = 0; i < 150; i++) {
            processor.onMessage(new MarketData("SYM" + i, BigDecimal.valueOf(100 + i), fakeClock.get()));
        }
        Map<String, Integer> debugMap = processor.debug();
        assertEquals(100, debugMap.get("publishedDataSetSize"));
        assertEquals(50, debugMap.get("pendingDataMapSize"));
    }

    @Test
    void testOnlyPublishOneceForTheSameSymbolInWindow() throws InterruptedException {
        // t=10
        processor.onMessage(new MarketData("A", BigDecimal.valueOf(1), 10));

        fakeClock.addAndGet(600);
        processor.onMessage(new MarketData("A", BigDecimal.valueOf(1), 30));
        assertEquals(1, processor.debug().get("pendingDataMapSize"));

        fakeClock.addAndGet(100);
        processor.onMessage(new MarketData("B", BigDecimal.valueOf(1), 40));
        assertEquals(2, processor.debug().get("publishedDataSetSize"));
        assertEquals(1, processor.debug().get("pendingDataMapSize"));

        fakeClock.addAndGet(700);
        // t=1410, the window for first A has passed
        processor.onMessage(new MarketData("A", BigDecimal.valueOf(1), 60));
        assertEquals(2, processor.debug().get("publishedDataSetSize"));
        assertEquals(0, processor.debug().get("pendingDataMapSize"));
    }

    @Test
    void testAlwaysPublishLatestData() {
        processor.onMessage(new MarketData("A", BigDecimal.valueOf(99.9), 20));
        processor.onMessage(new MarketData("A", BigDecimal.valueOf(1), 10));
        assertEquals(BigDecimal.valueOf(99.9), dataRepo.get("A").price());

        processor.onMessage(new MarketData("A", BigDecimal.valueOf(2), 50));
        assertEquals(BigDecimal.valueOf(2), dataRepo.get("A").price());

        processor.onMessage(new MarketData("A", BigDecimal.valueOf(4), 40));
        assertEquals(BigDecimal.valueOf(2), dataRepo.get("A").price());
    }
}
