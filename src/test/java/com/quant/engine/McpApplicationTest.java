package com.quant.engine;

import com.quant.engine.mcp.AlphaFeatureMcpGateway;
import com.quant.engine.model.MarketTick;
import com.quant.engine.service.AlpacaWebSocketClient;
import com.quant.engine.service.BinanceWebSocketClient;
import com.quant.engine.service.RealtimeMarketDataService;
import io.modelcontextprotocol.server.McpSyncServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class McpApplicationTest {

    @Autowired
    private AlphaFeatureMcpGateway gateway;

    @Autowired
    private RealtimeMarketDataService marketDataService;

    @Autowired
    private McpSyncServer mcpSyncServer;

    @Autowired
    private ToolCallbackProvider toolCallbackProvider;

    @Autowired
    private AlpacaWebSocketClient alpacaWebSocketClient;

    @Autowired
    private BinanceWebSocketClient binanceWebSocketClient;

    @org.springframework.boot.test.mock.mockito.MockBean
    private org.springframework.kafka.core.KafkaTemplate<String, MarketTick> kafkaTemplate;

    @Test
    @DisplayName("Context loads, live WebSocket ingestion clients and MCP tools are registered")
    void contextLoads() {
        assertNotNull(gateway, "Gateway bean should be registered");
        assertNotNull(marketDataService, "RealtimeMarketDataService bean should be registered");
        assertNotNull(mcpSyncServer, "McpSyncServer should be auto-configured");
        assertNotNull(toolCallbackProvider, "ToolCallbackProvider should be auto-configured");
        assertNotNull(alpacaWebSocketClient, "AlpacaWebSocketClient should be registered");
        assertNotNull(binanceWebSocketClient, "BinanceWebSocketClient should be registered");

        // Verify native MCP auto-registration of getHistoricalVwap and submitOrder tools
        var callbacks = toolCallbackProvider.getToolCallbacks();
        assertEquals(2, callbacks.length);
        var toolNames = java.util.Arrays.stream(callbacks)
                .map(c -> c.getToolDefinition().name())
                .collect(java.util.stream.Collectors.toSet());
        assertTrue(toolNames.contains("getHistoricalVwap"));
        assertTrue(toolNames.contains("submitOrder"));
        assertNotNull(mcpSyncServer.getServerCapabilities().tools());

        // Verify market data service ingestion and downsample calculation
        long now = System.currentTimeMillis();
        marketDataService.ingest(new MarketTick("PLTR", now - 2000, 1_502_500L, 100.0));
        marketDataService.ingest(new MarketTick("BTCUSDT", now - 1000, 9_000_000_000L, 0.5));

        assertTrue(marketDataService.size() >= 2, "Market data service should contain ingested ticks");

        var pltrResponse = gateway.getHistoricalVwap("PLTR", "now-1m", "now", 5);
        assertNotNull(pltrResponse);
        assertEquals("PLTR", pltrResponse.symbol());
        assertTrue(!pltrResponse.bars().isEmpty());

        var btcResponse = gateway.getHistoricalVwap("BTCUSDT", "now-1m", "now", 5);
        assertNotNull(btcResponse);
        assertEquals("BTCUSDT", btcResponse.symbol());
        assertTrue(!btcResponse.bars().isEmpty());
    }
}
