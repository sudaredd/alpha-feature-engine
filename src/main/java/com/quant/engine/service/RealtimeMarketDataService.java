package com.quant.engine.service;

import com.quant.engine.model.MarketTick;
import com.quant.engine.model.TimeBar;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory temporal market data store and aggregation service.
 * Stores ticks indexed by epoch milliseconds with a rolling 30-minute eviction window,
 * and aggregates high-frequency ticks into OHLCV and VWAP time bars.
 */
@Service
public class RealtimeMarketDataService {

    public static final double PRICE_SCALE = 10000.0;
    private static final long RETENTION_WINDOW_MS = 30 * 60 * 1000L; // 30 minutes

    private final ConcurrentSkipListMap<Long, List<MarketTick>> ticksByTimestamp = new ConcurrentSkipListMap<>();

    /**
     * Ingests a new market tick into the temporal store and strictly evicts any data
     * older than 30 minutes.
     *
     * @param tick MarketTick to ingest
     */
    public void ingest(MarketTick tick) {
        if (tick == null) {
            return;
        }

        long timestampMs = tick.timestampMs();
        ticksByTimestamp.computeIfAbsent(timestampMs, _ -> new CopyOnWriteArrayList<>()).add(tick);

        // Evict ticks older than 30 minutes relative to the current tick timestamp
        long cutoffMs = timestampMs - RETENTION_WINDOW_MS;
        ticksByTimestamp.headMap(cutoffMs, false).clear();
    }

    /**
     * Extracts ticks in the requested [startMs, endMs] range and downsamples them into
     * discrete TimeBar objects of size bucketSeconds.
     *
     * @param symbol        Ticker symbol
     * @param startMs       Epoch millisecond start time (inclusive)
     * @param endMs         Epoch millisecond end time (inclusive)
     * @param bucketSeconds Resolution of aggregated bars in seconds
     * @return List of TimeBar objects sorted chronologically
     */
    public List<TimeBar> getDownsampledBars(String symbol, long startMs, long endMs, int bucketSeconds) {
        if (bucketSeconds <= 0) {
            throw new IllegalArgumentException("bucketSeconds must be positive, received: " + bucketSeconds);
        }
        if (startMs > endMs || symbol == null || symbol.isBlank()) {
            return Collections.emptyList();
        }

        long bucketIntervalMs = bucketSeconds * 1000L;
        var subMap = ticksByTimestamp.subMap(startMs, true, endMs, true);

        // Map-reduce: group ticks by discrete bucket start epoch millisecond
        LinkedHashMap<Long, List<MarketTick>> buckets = new LinkedHashMap<>();
        for (List<MarketTick> tickList : subMap.values()) {
            for (MarketTick tick : tickList) {
                if (symbol.equalsIgnoreCase(tick.symbol())) {
                    long bucketKey = tick.timestampMs() - (tick.timestampMs() % bucketIntervalMs);
                    buckets.computeIfAbsent(bucketKey, _ -> new ArrayList<>()).add(tick);
                }
            }
        }

        if (buckets.isEmpty()) {
            return Collections.emptyList();
        }

        List<TimeBar> resultBars = new ArrayList<>(buckets.size());
        for (var entry : buckets.entrySet()) {
            long bucketStartMs = entry.getKey();
            List<MarketTick> ticks = entry.getValue();
            if (ticks.isEmpty()) {
                continue;
            }

            double open = ticks.getFirst().price() / PRICE_SCALE;
            double close = ticks.getLast().price() / PRICE_SCALE;
            long highRaw = Long.MIN_VALUE;
            long lowRaw = Long.MAX_VALUE;
            double totalVolume = 0.0;
            double sumWeightedPrice = 0.0;

            for (MarketTick t : ticks) {
                long priceRaw = t.price();
                double vol = t.volume();

                if (priceRaw > highRaw) highRaw = priceRaw;
                if (priceRaw < lowRaw) lowRaw = priceRaw;

                totalVolume += vol;
                sumWeightedPrice += (priceRaw / PRICE_SCALE) * vol;
            }

            double high = highRaw / PRICE_SCALE;
            double low = lowRaw / PRICE_SCALE;
            double vwap = totalVolume > 0.0 ? (sumWeightedPrice / totalVolume) : close;

            resultBars.add(new TimeBar(
                    Instant.ofEpochMilli(bucketStartMs),
                    open,
                    high,
                    low,
                    close,
                    vwap,
                    totalVolume
            ));
        }

        return Collections.unmodifiableList(resultBars);
    }

    /**
     * Returns the epoch millisecond of the oldest tick currently retained in memory.
     */
    public Long getOldestTimestamp() {
        return ticksByTimestamp.isEmpty() ? null : ticksByTimestamp.firstKey();
    }

    /**
     * Returns the epoch millisecond of the newest tick currently ingested.
     */
    public Long getNewestTimestamp() {
        return ticksByTimestamp.isEmpty() ? null : ticksByTimestamp.lastKey();
    }

    /**
     * For inspection and unit testing: returns current total number of unique timestamp buckets.
     */
    public int size() {
        return ticksByTimestamp.size();
    }

    /**
     * Clears all stored data (useful in testing).
     */
    public void clear() {
        ticksByTimestamp.clear();
    }
}
