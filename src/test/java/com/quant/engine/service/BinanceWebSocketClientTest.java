package com.quant.engine.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.engine.model.TimeBar;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BinanceWebSocketClientTest {

    private RealtimeMarketDataService marketDataService;
    private BinanceWebSocketClient client;

    @BeforeEach
    void setUp() {
        marketDataService = new RealtimeMarketDataService();
        client = new BinanceWebSocketClient(
                marketDataService,
                new ObjectMapper(),
                "BTCUSDT,ETHUSDT",
                "wss://stream.binance.com:9443/ws"
        );
    }

    @Test
    @DisplayName("Should parse direct Binance trade event and aggregate fractional crypto volume")
    void testProcessBinanceTradeDirect() {
        long tradeTime = 1788819000000L;
        String tradePayload = """
                {
                  "e": "trade",
                  "E": 1788819000100,
                  "s": "BTCUSDT",
                  "t": 1234567,
                  "p": "95000.50",
                  "q": "0.125",
                  "b": 88,
                  "a": 50,
                  "T": 1788819000000,
                  "m": true,
                  "M": true
                }
                """;

        client.processMessage(tradePayload);

        List<TimeBar> bars = marketDataService.getDownsampledBars("BTCUSDT", tradeTime, tradeTime + 5000, 5);
        assertEquals(1, bars.size());
        TimeBar bar = bars.getFirst();

        assertEquals(95000.50, bar.open(), 0.01);
        assertEquals(95000.50, bar.close(), 0.01);
        assertEquals(0.125, bar.totalVolume(), 0.0001);
        assertEquals(95000.50, bar.vwap(), 0.01);
    }

    @Test
    @DisplayName("Should parse combined stream payload format")
    void testProcessCombinedStreamFormat() {
        long tradeTime = 1788819100000L;
        String combinedPayload = """
                {
                  "stream": "ethusdt@trade",
                  "data": {
                    "e": "trade",
                    "E": 1788819100050,
                    "s": "ETHUSDT",
                    "t": 987654,
                    "p": "3450.25",
                    "q": "2.500",
                    "T": 1788819100000
                  }
                }
                """;

        client.processMessage(combinedPayload);

        List<TimeBar> bars = marketDataService.getDownsampledBars("ETHUSDT", tradeTime, tradeTime + 5000, 5);
        assertEquals(1, bars.size());
        TimeBar bar = bars.getFirst();

        assertEquals(3450.25, bar.open(), 0.01);
        assertEquals(3450.25, bar.close(), 0.01);
        assertEquals(2.50, bar.totalVolume(), 0.0001);
        assertEquals(3450.25, bar.vwap(), 0.01);
    }
}
