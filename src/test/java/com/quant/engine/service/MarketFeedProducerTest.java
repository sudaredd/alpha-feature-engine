package com.quant.engine.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarketFeedProducerTest {

    @Test
    @DisplayName("Should generate random-walk ticks around base price 1502500L")
    void testTickGeneration() {
        RealtimeMarketDataService service = new RealtimeMarketDataService();
        MarketFeedProducer producer = new MarketFeedProducer(service);

        // Generate 5 manual ticks
        for (int i = 0; i < 5; i++) {
            producer.generateTick();
        }

        assertTrue(service.size() >= 1);
        var bars = service.getDownsampledBars("PLTR", 0, System.currentTimeMillis() + 1000, 60);
        assertEquals(1, bars.size());
        assertTrue(bars.getFirst().open() > 140.0 && bars.getFirst().open() < 160.0);
        assertTrue(bars.getFirst().totalVolume() > 0);
    }
}
