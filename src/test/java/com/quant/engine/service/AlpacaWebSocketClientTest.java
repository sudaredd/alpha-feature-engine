package com.quant.engine.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.engine.model.MarketTick;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class AlpacaWebSocketClientTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, MarketTick> kafkaTemplate = mock(KafkaTemplate.class);
    private AlpacaWebSocketClient client;

    @BeforeEach
    void setUp() {
        client = new AlpacaWebSocketClient(
                kafkaTemplate,
                new ObjectMapper(),
                "test_key",
                "test_secret",
                "PLTR,QQQ",
                "wss://stream.data.alpaca.markets/v2/iex"
        );
    }

    @Test
    @DisplayName("Should parse Alpaca trade payload and publish MarketTick to Kafka topic market.ticks")
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

        ArgumentCaptor<MarketTick> tickCaptor = ArgumentCaptor.forClass(MarketTick.class);
        verify(kafkaTemplate, times(2)).send(eq("market.ticks"), eq("PLTR"), tickCaptor.capture());

        var ticks = tickCaptor.getAllValues();
        assertEquals(2, ticks.size());

        MarketTick tick1 = ticks.get(0);
        assertEquals("PLTR", tick1.symbol());
        assertEquals(1507500L, tick1.price());
        assertEquals(250.0, tick1.volume());

        MarketTick tick2 = ticks.get(1);
        assertEquals("PLTR", tick2.symbol());
        assertEquals(1512500L, tick2.price());
        assertEquals(150.0, tick2.volume());
    }

    @Test
    @DisplayName("Should handle system handshake and error messages gracefully without publishing to Kafka")
    void testProcessHandshakeAndErrors() {
        String connectedMsg = """
                [{"T":"success","msg":"connected"}]
                """;
        client.processMessage(connectedMsg);

        String errorMsg = """
                [{"T":"error","code":402,"msg":"auth failed"}]
                """;
        client.processMessage(errorMsg);

        verify(kafkaTemplate, never()).send(eq("market.ticks"), org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any(MarketTick.class));
    }
}
