package com.quant.engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.engine.model.MarketTick;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Real-time WebSocket ingestion client for Binance public market data (Crypto).
 * Streams live trade events (@trade) for configured crypto pairs, normalizes them
 * with fractional volume into MarketTick, and feeds RealtimeMarketDataService.
 */
@Service
public class BinanceWebSocketClient implements WebSocket.Listener {

    private static final Logger log = LoggerFactory.getLogger(BinanceWebSocketClient.class);

    private final RealtimeMarketDataService marketDataService;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService reconnectScheduler;
    private final HttpClient httpClient;

    private final String symbols;
    private final String wsUrl;

    private WebSocket webSocket;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final StringBuilder messageBuffer = new StringBuilder();

    public BinanceWebSocketClient(
            RealtimeMarketDataService marketDataService,
            ObjectMapper objectMapper,
            @Value("${binance.symbols:BTCUSDT,ETHUSDT,SOLUSDT}") String symbols,
            @Value("${binance.ws-url:wss://stream.binance.com:9443/ws}") String wsUrl) {

        this.marketDataService = marketDataService;
        this.objectMapper = objectMapper;
        this.symbols = symbols;
        this.wsUrl = wsUrl;

        this.reconnectScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "binance-ws-reconnect");
            t.setDaemon(true);
            return t;
        });

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @PostConstruct
    public void start() {
        isRunning.set(true);
        log.info("[BinanceWebSocketClient] Initializing live Binance crypto trade stream for [{}] at {}", symbols, wsUrl);
        connectAsync();
    }

    private void connectAsync() {
        if (!isRunning.get()) {
            return;
        }

        try {
            httpClient.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .buildAsync(URI.create(wsUrl), this)
                    .whenComplete((ws, throwable) -> {
                        if (throwable != null) {
                            log.warn("[BinanceWebSocketClient] Connection failed: {}. Scheduling reconnect in 10s...", throwable.getMessage());
                            scheduleReconnect(10);
                        } else {
                            this.webSocket = ws;
                            log.info("[BinanceWebSocketClient] WebSocket connection established.");
                        }
                    });
        } catch (Exception e) {
            log.warn("[BinanceWebSocketClient] Error initiating connection: {}. Reconnecting in 10s...", e.getMessage());
            scheduleReconnect(10);
        }
    }

    private void scheduleReconnect(int delaySeconds) {
        if (isRunning.get()) {
            reconnectScheduler.schedule(this::connectAsync, delaySeconds, TimeUnit.SECONDS);
        }
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        log.info("[BinanceWebSocketClient] Channel opened. Subscribing to trade streams...");
        subscribeStreams();
        webSocket.request(1);
    }

    private void subscribeStreams() {
        if (webSocket == null) {
            return;
        }

        try {
            List<String> streams = Arrays.stream(symbols.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(s -> s.toLowerCase() + "@trade")
                    .toList();

            SubscriptionRequest request = new SubscriptionRequest("SUBSCRIBE", streams, 1);
            String payload = objectMapper.writeValueAsString(request);
            webSocket.sendText(payload, true);
            log.info("[BinanceWebSocketClient] Sent stream subscription: {}", streams);
        } catch (Exception e) {
            log.error("[BinanceWebSocketClient] Error sending subscription request", e);
        }
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        messageBuffer.append(data);
        if (last) {
            String fullMessage = messageBuffer.toString();
            messageBuffer.setLength(0);
            processMessage(fullMessage);
        }
        webSocket.request(1);
        return CompletableFuture.completedFuture(null);
    }

    void processMessage(String payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);

            // Handle combined stream wrapper {"stream":"...","data":{...}} if present
            JsonNode dataNode = root.has("data") ? root.get("data") : root;

            String eventType = dataNode.path("e").asText();
            if ("trade".equalsIgnoreCase(eventType)) {
                processTrade(dataNode);
            } else if (root.has("result") && root.get("result").isNull()) {
                log.info("[BinanceWebSocketClient] Subscription acknowledged (id={})", root.path("id").asInt());
            }
        } catch (Exception e) {
            log.debug("[BinanceWebSocketClient] Error processing message: {}", e.getMessage());
        }
    }

    void processTrade(JsonNode tradeNode) {
        try {
            String symbol = tradeNode.path("s").asText();
            String priceStr = tradeNode.path("p").asText();
            String quantityStr = tradeNode.path("q").asText();
            long tradeTime = tradeNode.path("T").asLong();

            if (tradeTime <= 0) {
                tradeTime = tradeNode.path("E").asLong(System.currentTimeMillis());
            }

            double priceDouble = Double.parseDouble(priceStr);
            double quantity = Double.parseDouble(quantityStr);
            long priceFixed = Math.round(priceDouble * RealtimeMarketDataService.PRICE_SCALE);

            MarketTick tick = new MarketTick(symbol, tradeTime, priceFixed, quantity);
            marketDataService.ingest(tick);

            log.trace("[BinanceWebSocketClient] Ingested tick: {} @ ${} (vol={})", symbol, priceDouble, quantity);
        } catch (Exception e) {
            log.debug("[BinanceWebSocketClient] Error parsing trade: {}", e.getMessage());
        }
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        log.warn("[BinanceWebSocketClient] WebSocket closed [code={}]: {}. Reconnecting...", statusCode, reason);
        scheduleReconnect(5);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        log.warn("[BinanceWebSocketClient] WebSocket error: {}. Reconnecting...", error.getMessage());
        scheduleReconnect(5);
    }

    @PreDestroy
    public void stop() {
        isRunning.set(false);
        reconnectScheduler.shutdownNow();
        if (webSocket != null) {
            webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "Application shutting down");
        }
        log.info("[BinanceWebSocketClient] Stopped.");
    }

    private record SubscriptionRequest(String method, List<String> params, int id) {}
}
