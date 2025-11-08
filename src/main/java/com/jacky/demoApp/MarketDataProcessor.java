package com.jacky.demoApp;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.logging.Logger;

import com.jacky.demoApp.model.MarketData;

record PublishedData(String symbol, long timestamp) {
};

public class MarketDataProcessor {
    private static final Logger logger = Logger.getLogger(MarketDataProcessor.class.getName());
    private static final long WINDOW_SIZE_MILLIS = 1000;
    private static final long MAX_DATA_PER_WINDOW = 100;
    ScheduledExecutorService scheduler;

    private final Map<String, MarketData> latestDataMap = new ConcurrentHashMap<>();
    private final Deque<PublishedData> publishTimestamps = new ArrayDeque<>();
    private final Map<String, Long> publishedSymbolMap = new ConcurrentHashMap<>();
    private final Map<String, MarketData> pendingDataMap = new ConcurrentHashMap<>();

    private final LongSupplier currentTimeSupplier;
    private Map<String, MarketData> dataRepo;

    public MarketDataProcessor() {
        this(System::currentTimeMillis, new ConcurrentHashMap<>());
    }

    public MarketDataProcessor(LongSupplier currentTimeSupplier, Map<String, MarketData> dataRepo) {
        this.currentTimeSupplier = currentTimeSupplier;
        this.dataRepo = dataRepo;
        startScheduler();
    }

    private void startScheduler() {
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.scheduler.scheduleAtFixedRate(this::publishMarketData, 100, 100, TimeUnit.MILLISECONDS);
    }

    public void shutdownScheduler() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("Scheduler shut down successfully.");
        }
    }

    public void onMessage(MarketData data) {

        validateData(data);

        updateLatestMarketData(data);

        publishMarketData();

        logger.info("On message done, pending data: " + pendingDataMap);
    }

    private final ReentrantLock lock = new ReentrantLock();

    private void publishMarketData() {
        long now = currentTimeSupplier.getAsLong();
        lock.lock();
        try {
            Iterator<PublishedData> iterator = publishTimestamps.iterator();
            while (iterator.hasNext()) {
                PublishedData pd = iterator.next();
                if (now - pd.timestamp() >= WINDOW_SIZE_MILLIS) {
                    publishedSymbolMap.remove(pd.symbol());
                    iterator.remove();
                } else {
                    break;
                }
            }
            while (publishTimestamps.size() < MAX_DATA_PER_WINDOW) {
                MarketData toPublish = findNextDataToPublish(now);
                if (toPublish == null) {
                    break;
                }
                publishAggregatedMarketData(toPublish);
                publishedSymbolMap.put(toPublish.symbol(), now);
                publishTimestamps.offer(new PublishedData(toPublish.symbol(), now));
                pendingDataMap.remove(toPublish.symbol());
                logger.info("Published data: " + toPublish);
            }
        } finally {
            lock.unlock();
        }
    }

    private MarketData findNextDataToPublish(long now) {
        return pendingDataMap.values()
                .stream()
                .filter(md -> {
                    Long lastPublishedTime = publishedSymbolMap.get(md.symbol());
                    return lastPublishedTime == null || now - lastPublishedTime >= WINDOW_SIZE_MILLIS;
                })
                .max(Comparator.naturalOrder())
                .orElse(null);
    }

    private void updateLatestMarketData(MarketData data) {
        lock.lock();
        try {
            latestDataMap.compute(data.symbol(), (sym, existing) -> {
                if (existing == null || data.timestamp() > existing.timestamp()) {
                    logger.info("Add new data into candidates: " + data);
                    pendingDataMap.put(sym, data);
                    return data;
                } else {
                    return existing;
                }
            });
        } finally {
            lock.unlock();
        }
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
                "publishedDataSetSize", publishedSymbolMap.size(),
                "publishTimestampsSize", publishTimestamps.size());
    }

}
