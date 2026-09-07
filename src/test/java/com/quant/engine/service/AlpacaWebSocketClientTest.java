package com.quant.engine.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.engine.model.TimeBar;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AlpacaWebSocketClientTest {

    private RealtimeMarketDataService marketDataService;
    private AlpacaWebSocketClient client;

    @BeforeEach
    void setUp() {
        marketDataService = new RealtimeMarketDataService();
        client = new AlpacaWebSocketClient(
                marketDataService,
                new ObjectMapper(),
                "test_key",
                "test_secret",
                "PLTR,QQQ",
                "wss://stream.data.alpaca.markets/v2/iex"
        );
    }

    @Test
    @DisplayName("Should parse Alpaca trade payload and ingest into RealtimeMarketDataService")
    void testProcessAlpacaTrade() {
        long baseTimeMs = 1_000_000L;
        String t1 = Instant.ofEpochMilli(baseTimeMs + 100).toString();
        String t2 = Instant.ofEpochMilli(baseTimeMs + 200).toString();

        String tradePayload = String.format("""
                [
                  {
                    "T": "t",
                    "S": "PLTR",
                    "i": 98765,
                    "x": "V",
                    "p": 150.75,
                    "s": 250,
                    "c": ["@"],
                    "t": "%s",
                    "z": "C"
                  },
                  {
                    "T": "t",
                    "S": "PLTR",
                    "i": 98766,
                    "x": "V",
                    "p": 151.25,
                    "s": 150,
                    "c": ["@"],
                    "t": "%s",
                    "z": "C"
                  }
                ]
                """, t1, t2);

        client.processMessage(tradePayload);

        List<TimeBar> bars = marketDataService.getDownsampledBars("PLTR", baseTimeMs, baseTimeMs + 4999, 5);
        assertEquals(1, bars.size());
        TimeBar bar = bars.getFirst();

        assertEquals(150.75, bar.open(), 0.001);
        assertEquals(151.25, bar.close(), 0.001);
        assertEquals(400.0, bar.totalVolume(), 0.001);

        // Expected VWAP: (150.75 * 250 + 151.25 * 150) / 400 = (37687.5 + 22687.5) / 400 = 60375 / 400 = 150.9375
        assertEquals(150.9375, bar.vwap(), 0.001);
    }

    @Test
    @DisplayName("Should handle system handshake and error messages gracefully without exception")
    void testProcessHandshakeAndErrors() {
        String connectedMsg = """
                [{"T":"success","msg":"connected"}]
                """;
        client.processMessage(connectedMsg);

        String errorMsg = """
                [{"T":"error","code":402,"msg":"auth failed"}]
                """;
        client.processMessage(errorMsg);

        // No exceptions thrown, market store unaffected
        assertEquals(0, marketDataService.size());
    }
}
