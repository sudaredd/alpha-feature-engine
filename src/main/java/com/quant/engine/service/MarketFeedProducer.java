package com.quant.engine.service;

import com.quant.engine.model.MarketTick;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-frequency market feed simulator.
 * Generates 20 random-walk ticks per second for "PLTR" with fixed-point arithmetic
 * starting at base $150.25 (1502500L).
 */
@Component
public class MarketFeedProducer {

    private static final Logger log = LoggerFactory.getLogger(MarketFeedProducer.class);

    public static final String SYMBOL = "PLTR";
    public static final long BASE_PRICE = 1502500L; // $150.25 in 10^4 scale

    private final RealtimeMarketDataService marketDataService;
    private final AtomicLong currentPrice = new AtomicLong(BASE_PRICE);
    private final AtomicLong tickCount = new AtomicLong(0);
    private ScheduledExecutorService executor;

    public MarketFeedProducer(RealtimeMarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    @PostConstruct
    public void startFeed() {
        log.info(">>> [MarketFeedProducer] STARTED: streaming 20 ticks/sec for {} at base price ${}",
                SYMBOL, String.format("%.2f", BASE_PRICE / 10000.0));

        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "market-feed-producer");
            t.setDaemon(true);
            return t;
        });

        // 20 ticks per second = 1 tick every 50 milliseconds
        executor.scheduleAtFixedRate(this::generateTick, 0, 50, TimeUnit.MILLISECONDS);
    }

    void generateTick() {
        try {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            // Random walk price variation: -500 to +500 ($0.05 variation per tick)
            long delta = random.nextLong(-500L, 501L);
            long price = currentPrice.addAndGet(delta);
            if (price < 10000L) { // Prevent zero or negative prices
                price = 10000L;
                currentPrice.set(price);
            }

            // Volume between 10 and 200 shares
            long volume = random.nextLong(10L, 201L);
            long nowMs = System.currentTimeMillis();

            MarketTick tick = new MarketTick(SYMBOL, nowMs, price, volume);
            marketDataService.ingest(tick);

            // Log a heartbeat once every second (every 20 ticks) so feed activity is clearly visible
            long count = tickCount.incrementAndGet();
            if (count % 20 == 0) {
                log.info("MarketFeed [{}] tick #{}: latest price=${}, volume={}, storeSize={}",
                        SYMBOL, count, String.format("%.2f", price / 10000.0), volume, marketDataService.size());
            }
        } catch (Exception e) {
            log.error("Error generating simulated market tick", e);
        }
    }

    @PreDestroy
    public void stopFeed() {
        if (executor != null) {
            log.info("Stopping MarketFeedProducer");
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException _) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
