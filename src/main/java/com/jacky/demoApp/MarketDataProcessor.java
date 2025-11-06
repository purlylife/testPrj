package com.jacky.demoApp;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.logging.Logger;

import com.jacky.demoApp.model.MarketData;

record PublishedData(String symbol, long timestamp) {
};

public class MarketDataProcessor {
    private static final Logger logger = Logger.getLogger(MarketDataProcessor.class.getName());
    private static final long WINDOW_SIZE_MILLIS = 1000;
    private static final long MAX_DATA_PER_WINDOW = 100;

    private final Map<String, MarketData> latestDataMap = new ConcurrentHashMap<>();
    private final Deque<PublishedData> publishTimestamps = new ArrayDeque<>();
    private final Set<String> publishedDataSet = ConcurrentHashMap.newKeySet();
    private final Map<String, MarketData> pendingDataMap = new ConcurrentHashMap<>();

    private final LongSupplier currentTimeSupplier;
    private Map<String, MarketData> dataRepo;

    public MarketDataProcessor(LongSupplier currentTimeSupplier, Map<String, MarketData> dataRepo) {
        this.currentTimeSupplier = currentTimeSupplier;
        this.dataRepo = dataRepo;
    }

    public void onMessage(MarketData data) {

        validateData(data);

        updateLatestMarketData(data);

        publishMarketData();

        logger.info("On message done, pending data: " + pendingDataMap);
    }

    private void publishMarketData() {
        long now = currentTimeSupplier.getAsLong();
        while (!publishTimestamps.isEmpty() && now - publishTimestamps.peekFirst().timestamp() >= WINDOW_SIZE_MILLIS) {
            publishedDataSet.remove(publishTimestamps.pollFirst().symbol());
        }

        while (publishTimestamps.size() < MAX_DATA_PER_WINDOW) {
            MarketData toPublish = findNextDataToPublish();
            if (toPublish == null) {
                break;
            }
            publishAggregatedMarketData(toPublish);
            publishedDataSet.add(toPublish.symbol());
            publishTimestamps.offer(new PublishedData(toPublish.symbol(), now));
            pendingDataMap.remove(toPublish.symbol());
            logger.info("Published data: " + toPublish);
        }
    }

    private MarketData findNextDataToPublish() {
        return pendingDataMap.values()
                .stream()
                .filter(md -> !publishedDataSet.contains(md.symbol()))
                .max(Comparator.naturalOrder())
                .orElse(null);
    }

    private void updateLatestMarketData(MarketData data) {
        latestDataMap.compute(data.symbol(), (sym, existing) -> {
            if (existing == null || data.timestamp() > existing.timestamp()) {
                logger.info("Add new data into candidates: " + data);
                pendingDataMap.put(sym, data);
                return data;
            } else {
                return existing;
            }
        });
    }

    public void publishAggregatedMarketData(MarketData data) {
        logger.info(data.toString());
        this.dataRepo.put(data.symbol(), data);
    }

    private void validateData(MarketData data) {
        // Basic validation checks
        if (data.symbol() == null || data.symbol().isEmpty()) {
            throw new IllegalArgumentException("Invalid symbol");
        }
        if (data.price() == null || data.price().compareTo(java.math.BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Invalid price");
        }
        if (data.timestamp() <= 0 || data.timestamp() > currentTimeSupplier.getAsLong()) {
            throw new IllegalArgumentException("Invalid timestamp");
        }
    }

    public Map<String, Integer> debug() {
        return Map.of(
                "latestDataMapSize", latestDataMap.size(),
                "pendingDataMapSize", pendingDataMap.size(),
                "publishedDataSetSize", publishedDataSet.size(),
                "publishTimestampsSize", publishTimestamps.size());
    }

}
