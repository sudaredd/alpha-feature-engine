package com.quant.engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.engine.model.MarketTick;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Free-tier Market Data Service powered by Alpha Vantage REST API.
 * Fetches real-world equity quotes (GLOBAL_QUOTE) and seeds RealtimeMarketDataService
 * with actual market prices and trading volume for configured tickers.
 */
@Service
public class AlphaVantageMarketDataService {

    private static final Logger log = LoggerFactory.getLogger(AlphaVantageMarketDataService.class);
    private static final String BASE_URL = "https://www.alphavantage.co/query";

    private final RealtimeMarketDataService marketDataService;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    private final String apiKey;
    private final String symbols;
    private final boolean enabled;

    public AlphaVantageMarketDataService(
            RealtimeMarketDataService marketDataService,
            ObjectMapper objectMapper,
            @Value("${alphavantage.api-key:default_key}") String apiKey,
            @Value("${alphavantage.symbols:PLTR,QQQ}") String symbols,
            @Value("${alphavantage.enabled:true}") boolean enabled) {

        this.marketDataService = marketDataService;
        this.objectMapper = objectMapper;
        this.apiKey = apiKey;
        this.symbols = symbols;
        this.enabled = enabled;

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @PostConstruct
    public void init() {
        if (!enabled || apiKey == null || apiKey.isBlank() || "default_key".equalsIgnoreCase(apiKey)) {
            log.info("[AlphaVantage] Service disabled or API key not set.");
            return;
        }

        log.info("[AlphaVantage] Initializing real-time equity data seed for symbols: [{}]", symbols);
        CompletableFuture.runAsync(this::seedAllConfiguredSymbols);
    }

    public void seedAllConfiguredSymbols() {
        List<String> symbolList = Arrays.stream(symbols.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();

        for (String sym : symbolList) {
            fetchAndIngestGlobalQuote(sym);
            try {
                // Respect free-tier rate limit (up to 5 calls/min)
                Thread.sleep(1200);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public boolean fetchAndIngestGlobalQuote(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return false;
        }

        String normalizedSymbol = symbol.trim().toUpperCase();
        String uriStr = String.format("%s?function=GLOBAL_QUOTE&symbol=%s&apikey=%s",
                BASE_URL, normalizedSymbol, apiKey);

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(uriStr))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.warn("[AlphaVantage] Failed to fetch quote for {}: HTTP {}", normalizedSymbol, response.statusCode());
                return false;
            }

            JsonNode root = objectMapper.readTree(response.body());
            JsonNode quote = root.path("Global Quote");

            if (quote.isMissingNode() || quote.isEmpty()) {
                log.warn("[AlphaVantage] No 'Global Quote' found in response for {}: {}", normalizedSymbol, response.body());
                return false;
            }

            String priceStr = quote.path("05. price").asText();
            String volumeStr = quote.path("06. volume").asText();

            if (priceStr.isBlank()) {
                return false;
            }

            double priceDouble = Double.parseDouble(priceStr);
            double volume = volumeStr.isBlank() ? 100.0 : Double.parseDouble(volumeStr);
            long priceFixed = Math.round(priceDouble * RealtimeMarketDataService.PRICE_SCALE);
            long nowMs = System.currentTimeMillis();

            MarketTick tick = new MarketTick(normalizedSymbol, nowMs, priceFixed, volume);
            marketDataService.ingest(tick);

            log.info("[AlphaVantage] Successfully ingested real market quote for {}: ${} (volume={})",
                    normalizedSymbol, String.format("%.2f", priceDouble), volume);
            return true;

        } catch (Exception e) {
            log.error("[AlphaVantage] Error querying quote for {}: {}", normalizedSymbol, e.getMessage());
            return false;
        }
    }
}
