package com.quant.engine;

import com.quant.engine.mcp.AlphaFeatureMcpGateway;
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

    @Test
    @DisplayName("Context loads, beans are registered, and live market feed is producing ticks")
    void contextLoads() throws InterruptedException {
        assertNotNull(gateway, "Gateway bean should be registered");
        assertNotNull(marketDataService, "RealtimeMarketDataService bean should be registered");
        assertNotNull(mcpSyncServer, "McpSyncServer should be auto-configured");
        assertNotNull(toolCallbackProvider, "ToolCallbackProvider should be auto-configured");

        // Verify native MCP auto-registration of getHistoricalVwap and submitOrder tools
        var callbacks = toolCallbackProvider.getToolCallbacks();
        assertEquals(2, callbacks.length);
        var toolNames = java.util.Arrays.stream(callbacks)
                .map(c -> c.getToolDefinition().name())
                .collect(java.util.stream.Collectors.toSet());
        assertTrue(toolNames.contains("getHistoricalVwap"));
        assertTrue(toolNames.contains("submitOrder"));
        assertNotNull(mcpSyncServer.getServerCapabilities().tools());

        // Allow feed producer to emit ticks (20 ticks/sec -> 50ms interval)
        Thread.sleep(150);

        assertTrue(marketDataService.size() > 0, "Market feed producer should have ingested ticks");

        // Verify downsample query on live streamed ticks
        long now = System.currentTimeMillis();
        var bars = marketDataService.getDownsampledBars("PLTR", now - 5000, now + 1000, 1);
        assertNotNull(bars);
        assertTrue(!bars.isEmpty(), "Downsampled bars should be generated from live ticks");
    }
}
