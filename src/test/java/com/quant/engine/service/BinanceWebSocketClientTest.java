package com.quant.engine.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.engine.model.MarketTick;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class BinanceWebSocketClientTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, MarketTick> kafkaTemplate = mock(KafkaTemplate.class);
    private BinanceWebSocketClient client;

    @BeforeEach
    void setUp() {
        client = new BinanceWebSocketClient(
                kafkaTemplate,
                new ObjectMapper(),
                "BTCUSDT,ETHUSDT",
                "wss://stream.binance.us:9443/ws"
        );
    }

    @Test
    @DisplayName("Should parse direct Binance trade event and publish MarketTick to Kafka topic market.ticks")
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

        ArgumentCaptor<MarketTick> captor = ArgumentCaptor.forClass(MarketTick.class);
        verify(kafkaTemplate).send(eq("market.ticks"), eq("BTCUSDT"), captor.capture());

        MarketTick tick = captor.getValue();
        assertEquals("BTCUSDT", tick.symbol());
        assertEquals(tradeTime, tick.timestampMs());
        assertEquals(950005000L, tick.price());
        assertEquals(0.125, tick.volume(), 0.0001);
    }

    @Test
    @DisplayName("Should parse combined stream payload format and publish to Kafka")
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

        ArgumentCaptor<MarketTick> captor = ArgumentCaptor.forClass(MarketTick.class);
        verify(kafkaTemplate).send(eq("market.ticks"), eq("ETHUSDT"), captor.capture());

        MarketTick tick = captor.getValue();
        assertEquals("ETHUSDT", tick.symbol());
        assertEquals(tradeTime, tick.timestampMs());
        assertEquals(34502500L, tick.price());
        assertEquals(2.50, tick.volume(), 0.0001);
    }
}
