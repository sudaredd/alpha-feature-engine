package com.quant.engine.service;

import com.quant.engine.model.MarketTick;
import com.quant.engine.model.TimeBar;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RealtimeMarketDataServiceTest {

    private RealtimeMarketDataService service;

    @BeforeEach
    void setUp() {
        service = new RealtimeMarketDataService();
    }

    @Test
    @DisplayName("Should ingest ticks and accurately calculate OHLCV and VWAP time bars")
    void testDownsampledBarsCalculation() {
        // Base time: 1000000 ms
        // 5-second bucket (bucketInterval = 5000 ms)
        // Bucket 1: [1000000, 1005000)
        long baseTime = 1_000_000L;

        // Tick 1: price 150.00 (1500000L), vol 100 at baseTime + 100
        service.ingest(new MarketTick("PLTR", baseTime + 100, 1_500_000L, 100.0));
        // Tick 2: price 152.50 (1525000L), vol 200 at baseTime + 500
        service.ingest(new MarketTick("PLTR", baseTime + 500, 1_525_000L, 200.0));
        // Tick 3: price 149.00 (1490000L), vol 100 at baseTime + 1200
        service.ingest(new MarketTick("PLTR", baseTime + 1200, 1_490_000L, 100.0));
        // Tick 4: price 151.00 (1510000L), vol 100 at baseTime + 2000
        service.ingest(new MarketTick("PLTR", baseTime + 2000, 1_510_000L, 100.0));

        // Different symbol in the same bucket - should be ignored
        service.ingest(new MarketTick("AAPL", baseTime + 2500, 2_000_000L, 500.0));

        // Query for PLTR with 5-second resolution
        List<TimeBar> bars = service.getDownsampledBars("PLTR", baseTime, baseTime + 4999, 5);

        assertEquals(1, bars.size());
        TimeBar bar = bars.getFirst();

        assertEquals(Instant.ofEpochMilli(baseTime), bar.timestamp());
        assertEquals(150.00, bar.open(), 0.0001);
        assertEquals(152.50, bar.high(), 0.0001);
        assertEquals(149.00, bar.low(), 0.0001);
        assertEquals(151.00, bar.close(), 0.0001);

        double expectedTotalVolume = 100.0 + 200.0 + 100.0 + 100.0; // 500.0
        assertEquals(expectedTotalVolume, bar.totalVolume(), 0.0001);

        // Expected VWAP: (150*100 + 152.5*200 + 149*100 + 151*100) / 500
        // = (15000 + 30500 + 14900 + 15100) / 500 = 75500 / 500 = 151.00
        double expectedVwap = (150.0 * 100 + 152.5 * 200 + 149.0 * 100 + 151.0 * 100) / 500.0;
        assertEquals(expectedVwap, bar.vwap(), 0.0001);
    }

    @Test
    @DisplayName("Should accurately aggregate fractional crypto volumes and calculate VWAP")
    void testFractionalCryptoVolumeVwap() {
        long baseTime = 2_000_000L;

        // BTC trade 1: $90,000 (900000000L), vol 0.15 BTC
        service.ingest(new MarketTick("BTCUSDT", baseTime + 100, 900_000_000L, 0.15));
        // BTC trade 2: $91,000 (910000000L), vol 0.35 BTC
        service.ingest(new MarketTick("BTCUSDT", baseTime + 300, 910_000_000L, 0.35));

        List<TimeBar> bars = service.getDownsampledBars("BTCUSDT", baseTime, baseTime + 4999, 5);
        assertEquals(1, bars.size());
        TimeBar bar = bars.getFirst();

        assertEquals(90000.0, bar.open(), 0.0001);
        assertEquals(91000.0, bar.close(), 0.0001);
        assertEquals(0.50, bar.totalVolume(), 0.00001);

        // VWAP = (90000 * 0.15 + 91000 * 0.35) / 0.50 = (13500 + 31850) / 0.5 = 45350 / 0.5 = 90700.0
        assertEquals(90700.0, bar.vwap(), 0.0001);
    }

    @Test
    @DisplayName("Should segment ticks across multiple sequential time buckets")
    void testMultipleBuckets() {
        long bucket1Time = 10_000L; // 10 seconds
        long bucket2Time = 20_000L; // 20 seconds

        service.ingest(new MarketTick("PLTR", bucket1Time + 100, 1_000_000L, 50.0));
        service.ingest(new MarketTick("PLTR", bucket2Time + 200, 1_100_000L, 75.0));

        List<TimeBar> bars = service.getDownsampledBars("PLTR", bucket1Time, bucket2Time + 5000, 10);
        assertEquals(2, bars.size());

        assertEquals(Instant.ofEpochMilli(bucket1Time), bars.get(0).timestamp());
        assertEquals(100.0, bars.get(0).close(), 0.0001);
        assertEquals(50.0, bars.get(0).totalVolume(), 0.0001);

        assertEquals(Instant.ofEpochMilli(bucket2Time), bars.get(1).timestamp());
        assertEquals(110.0, bars.get(1).close(), 0.0001);
        assertEquals(75.0, bars.get(1).totalVolume(), 0.0001);
    }

    @Test
    @DisplayName("Should strictly evict data older than 30 minutes")
    void testEvictionPolicy() {
        long t0 = 100_000L;
        service.ingest(new MarketTick("PLTR", t0, 1_500_000L, 100.0));
        assertEquals(1, service.size());

        // Ingest tick 29 minutes later: no eviction should occur
        long t29Min = t0 + (29 * 60 * 1000L);
        service.ingest(new MarketTick("PLTR", t29Min, 1_505_000L, 100.0));
        assertEquals(2, service.size());

        // Ingest tick 31 minutes after t0: t0 should be strictly evicted
        long t31Min = t0 + (31 * 60 * 1000L);
        service.ingest(new MarketTick("PLTR", t31Min, 1_510_000L, 100.0));

        // Querying for t0 should return no bars
        List<TimeBar> oldBars = service.getDownsampledBars("PLTR", t0, t0 + 1000, 1);
        assertTrue(oldBars.isEmpty(), "Data older than 30 minutes should have been evicted");

        // Querying for recent ticks should still return bars
        List<TimeBar> recentBars = service.getDownsampledBars("PLTR", t29Min, t31Min + 1000, 60);
        assertNotNull(recentBars);
        assertEquals(2, recentBars.size());
    }

    @Test
    @DisplayName("Should return empty list for invalid or out-of-range queries")
    void testEdgeCases() {
        List<TimeBar> emptyRange = service.getDownsampledBars("PLTR", 5000, 1000, 5);
        assertTrue(emptyRange.isEmpty());

        List<TimeBar> nullSymbol = service.getDownsampledBars(null, 1000, 5000, 5);
        assertTrue(nullSymbol.isEmpty());
    }
}
