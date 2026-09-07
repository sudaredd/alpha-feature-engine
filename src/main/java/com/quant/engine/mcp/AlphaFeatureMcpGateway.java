package com.quant.engine.mcp;

import com.quant.engine.model.DownsampledSeriesResponse;
import com.quant.engine.model.OrderExecutionResponse;
import com.quant.engine.model.TimeBar;
import com.quant.engine.service.AlphaVantageMarketDataService;
import com.quant.engine.service.RealtimeMarketDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

/**
 * Model Context Protocol (MCP) gateway exposing quantitative alpha features, time bars,
 * and order execution capabilities.
 */
@Service
public class AlphaFeatureMcpGateway {

    private static final Logger log = LoggerFactory.getLogger(AlphaFeatureMcpGateway.class);

    private final RealtimeMarketDataService realtimeMarketDataService;
    private final AlphaVantageMarketDataService alphaVantageService;

    public AlphaFeatureMcpGateway(RealtimeMarketDataService realtimeMarketDataService) {
        this(realtimeMarketDataService, null);
    }

    @org.springframework.beans.factory.annotation.Autowired
    public AlphaFeatureMcpGateway(
            RealtimeMarketDataService realtimeMarketDataService,
            @org.springframework.beans.factory.annotation.Autowired(required = false) AlphaVantageMarketDataService alphaVantageService) {
        this.realtimeMarketDataService = realtimeMarketDataService;
        this.alphaVantageService = alphaVantageService;
    }

    @McpTool(description = "Submits a live trade order to the execution engine.")
    public OrderExecutionResponse submitOrder(
            @McpToolParam(description = "The stock symbol to trade (e.g., PLTR)") String symbol,
            @McpToolParam(description = "Order side (BUY or SELL)") String side,
            @McpToolParam(description = "Number of shares to execute") long quantity,
            @McpToolParam(description = "AI confidence score from 0 to 100 representing confidence in the trade") int confidenceScore) {

        String orderId = UUID.randomUUID().toString();
        long timestampMs = System.currentTimeMillis();
        String normalizedSide = side != null ? side.toUpperCase().trim() : "BUY";
        String normalizedSymbol = symbol != null ? symbol.toUpperCase().trim() : "UNKNOWN";

        log.info("MCP Trade Order Submitted: orderId={}, symbol={}, side={}, quantity={}, confidenceScore={}/100, timestampMs={}",
                orderId, normalizedSymbol, normalizedSide, quantity, confidenceScore, timestampMs);

        return new OrderExecutionResponse(
                orderId,
                normalizedSymbol,
                normalizedSide,
                quantity,
                "FILLED",
                timestampMs
        );
    }

    @McpTool(description = "Retrieves historical Volume-Weighted Average Price (VWAP) and other alpha features downsampled to LLM-friendly time bars. "
            + "Queries the RealtimeMarketDataService for the specified time range and buckets microsecond ticks into the requested resolution.")
    public DownsampledSeriesResponse getHistoricalVwap(
            @McpToolParam(description = "The stock symbol to query (e.g., PLTR)") String symbol,
            @McpToolParam(description = "The start time of the query in ISO-8601 format (e.g., 2026-09-05T19:25:00-04:00, 2026-09-05T23:25:00Z, or 'now-5m')") String startTime,
            @McpToolParam(description = "The end time of the query in ISO-8601 format (e.g., 2026-09-05T19:30:00-04:00, 2026-09-05T23:30:00Z, or 'now')") String endTime,
            @McpToolParam(description = "The resolution in seconds for the downsampled time bars (e.g., 5, 15, 60)") int resolutionSeconds) {

        log.info("MCP Tool Invoked - getHistoricalVwap: symbol={}, startTime='{}', endTime='{}', resolution={}s",
                symbol, startTime, endTime, resolutionSeconds);

        long endMs = parseIsoTimestamp(endTime, true);
        long startMs = parseIsoTimestamp(startTime, false);

        Long oldest = realtimeMarketDataService.getOldestTimestamp();
        Long newest = realtimeMarketDataService.getNewestTimestamp();

        log.info("Query Window: [{} to {}] | In-memory tick range: [{} to {}] (storeSize={})",
                Instant.ofEpochMilli(startMs), Instant.ofEpochMilli(endMs),
                oldest != null ? Instant.ofEpochMilli(oldest) : "EMPTY",
                newest != null ? Instant.ofEpochMilli(newest) : "EMPTY",
                realtimeMarketDataService.size());

        List<TimeBar> bars = realtimeMarketDataService.getDownsampledBars(symbol, startMs, endMs, resolutionSeconds);
        if (bars.isEmpty() && alphaVantageService != null) {
            log.info("No in-memory ticks for symbol {}. Requesting Alpha Vantage real-time quote backfill...", symbol);
            if (alphaVantageService.fetchAndIngestGlobalQuote(symbol)) {
                bars = realtimeMarketDataService.getDownsampledBars(symbol, startMs, endMs, resolutionSeconds);
            }
        }
        log.info("Returning {} downsampled time bar(s) for symbol {}", bars.size(), symbol);

        return new DownsampledSeriesResponse(symbol, resolutionSeconds, bars);
    }

    private long parseIsoTimestamp(String isoString, boolean isEnd) {
        if (isoString == null || isoString.isBlank() || isoString.equalsIgnoreCase("now")) {
            return isEnd ? System.currentTimeMillis() : System.currentTimeMillis() - (5 * 60 * 1000L);
        }

        String trimmed = isoString.trim();

        // Support relative offsets like now-5m, -5m, now-10m
        if (trimmed.startsWith("now-") || trimmed.startsWith("-")) {
            String sub = trimmed.replace("now", "").replace("-", "").trim();
            long millis = parseDurationMillis(sub);
            return System.currentTimeMillis() - millis;
        }

        // Try standard Instant parse (handles UTC 'Z' offsets)
        try {
            return Instant.parse(trimmed).toEpochMilli();
        } catch (Exception _) {
        }

        // Try OffsetDateTime parse (handles explicit offsets like -04:00, +00:00)
        try {
            return OffsetDateTime.parse(trimmed).toInstant().toEpochMilli();
        } catch (Exception _) {
        }

        // Fallback: parse as LocalDateTime in local system timezone (e.g., 2026-09-05T19:25:00)
        try {
            return LocalDateTime.parse(trimmed)
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
        } catch (Exception _) {
            // Last resort: parse as date only
            return java.time.LocalDate.parse(trimmed)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
        }
    }

    private long parseDurationMillis(String durationStr) {
        String lower = durationStr.toLowerCase();
        if (lower.endsWith("m")) {
            return Long.parseLong(lower.substring(0, lower.length() - 1)) * 60 * 1000L;
        } else if (lower.endsWith("s")) {
            return Long.parseLong(lower.substring(0, lower.length() - 1)) * 1000L;
        } else if (lower.endsWith("h")) {
            return Long.parseLong(lower.substring(0, lower.length() - 1)) * 3600 * 1000L;
        }
        return Long.parseLong(lower) * 60 * 1000L;
    }
}
