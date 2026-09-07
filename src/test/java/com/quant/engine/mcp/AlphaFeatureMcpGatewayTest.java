package com.quant.engine.mcp;

import com.quant.engine.model.DownsampledSeriesResponse;
import com.quant.engine.model.MarketTick;
import com.quant.engine.service.RealtimeMarketDataService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class AlphaFeatureMcpGatewayTest {

    private RealtimeMarketDataService marketDataService;
    private AlphaFeatureMcpGateway gateway;

    @BeforeEach
    void setUp() {
        marketDataService = new RealtimeMarketDataService();
        gateway = new AlphaFeatureMcpGateway(marketDataService);
    }

    @Test
    @DisplayName("Should query getHistoricalVwap with ISO-8601 strings and return DownsampledSeriesResponse")
    void testGetHistoricalVwap() {
        Instant t0 = Instant.parse("2026-09-05T19:00:00Z");
        long t0Ms = t0.toEpochMilli();

        // Ingest simulated ticks
        marketDataService.ingest(new MarketTick("PLTR", t0Ms + 1000, 1_502_500L, 100));
        marketDataService.ingest(new MarketTick("PLTR", t0Ms + 2000, 1_505_000L, 200));

        DownsampledSeriesResponse response = gateway.getHistoricalVwap(
                "PLTR",
                "2026-09-05T19:00:00Z",
                "2026-09-05T19:01:00Z",
                60
        );

        assertNotNull(response);
        assertEquals("PLTR", response.symbol());
        assertEquals(60, response.resolutionSeconds());
        assertFalse(response.bars().isEmpty());

        var bar = response.bars().getFirst();
        assertEquals(150.25, bar.open(), 0.001);
        assertEquals(150.50, bar.high(), 0.001);
        assertEquals(150.25, bar.low(), 0.001);
        assertEquals(150.50, bar.close(), 0.001);
        assertEquals(300, bar.totalVolume());
    }

    @Test
    @DisplayName("Should submit order and return OrderExecutionResponse with FILLED status and UUID")
    void testSubmitOrder() {
        var response = gateway.submitOrder("PLTR", "BUY", 500, 95);

        assertNotNull(response);
        assertNotNull(response.orderId());
        assertEquals("PLTR", response.symbol());
        assertEquals("BUY", response.side());
        assertEquals(500, response.quantity());
        assertEquals("FILLED", response.status());
        org.junit.jupiter.api.Assertions.assertTrue(response.timestampMs() > 0);
    }
}
