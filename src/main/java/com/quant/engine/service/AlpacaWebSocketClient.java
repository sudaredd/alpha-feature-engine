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
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Real-time WebSocket ingestion client for Alpaca IEX Market Data (US Equities).
 * Connects to Alpaca v2 streaming endpoint, authenticates, subscribes to configured tickers,
 * normalizes trades to MarketTick, and feeds RealtimeMarketDataService.
 */
@Service
public class AlpacaWebSocketClient implements WebSocket.Listener {

    private static final Logger log = LoggerFactory.getLogger(AlpacaWebSocketClient.class);

    private final RealtimeMarketDataService marketDataService;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService reconnectScheduler;
    private final HttpClient httpClient;

    private final String apiKey;
    private final String apiSecret;
    private final String symbols;
    private final String wsUrl;

    private WebSocket webSocket;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean authFailed = new AtomicBoolean(false);
    private final StringBuilder messageBuffer = new StringBuilder();

    public AlpacaWebSocketClient(
            RealtimeMarketDataService marketDataService,
            ObjectMapper objectMapper,
            @Value("${alpaca.api-key:default_key}") String apiKey,
            @Value("${alpaca.api-secret:default_secret}") String apiSecret,
            @Value("${alpaca.symbols:PLTR,QQQ,VUG,SPMO}") String symbols,
            @Value("${alpaca.ws-url:wss://stream.data.alpaca.markets/v2/iex}") String wsUrl) {

        this.marketDataService = marketDataService;
        this.objectMapper = objectMapper;
        this.apiKey = apiKey;
        this.apiSecret = apiSecret;
        this.symbols = symbols;
        this.wsUrl = wsUrl;

        this.reconnectScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "alpaca-ws-reconnect");
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
        if (isDefaultOrBlank(apiKey)) {
            log.warn("[AlpacaWebSocketClient] Alpaca credentials not configured (using placeholder 'default_key'). "
                    + "Set ALPACA_API_KEY and ALPACA_API_SECRET environment variables to stream live US equities.");
        }
        log.info("[AlpacaWebSocketClient] Initializing live Alpaca IEX stream for symbols: [{}] at {}", symbols, wsUrl);
        connectAsync();
    }

    private boolean isDefaultOrBlank(String key) {
        return key == null || key.isBlank() || "default_key".equalsIgnoreCase(key.trim());
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
                            log.warn("[AlpacaWebSocketClient] Connection failed: {}. Scheduling reconnect in 30s...", throwable.getMessage());
                            scheduleReconnect(30);
                        } else {
                            this.webSocket = ws;
                            log.info("[AlpacaWebSocketClient] WebSocket connection established.");
                        }
                    });
        } catch (Exception e) {
            log.warn("[AlpacaWebSocketClient] Error initiating connection: {}. Reconnecting in 30s...", e.getMessage());
            scheduleReconnect(30);
        }
    }

    private void scheduleReconnect(int delaySeconds) {
        if (isRunning.get() && !authFailed.get()) {
            reconnectScheduler.schedule(this::connectAsync, delaySeconds, TimeUnit.SECONDS);
        }
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        log.info("[AlpacaWebSocketClient] Channel opened, awaiting server handshake...");
        webSocket.request(1);
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
            if (root.isArray()) {
                for (JsonNode node : root) {
                    handleMessageNode(node);
                }
            } else {
                handleMessageNode(root);
            }
        } catch (Exception e) {
            log.debug("[AlpacaWebSocketClient] Error processing JSON payload: {}", e.getMessage());
        }
    }

    private void handleMessageNode(JsonNode node) {
        String msgType = node.path("T").asText();

        switch (msgType) {
            case "success" -> {
                String msg = node.path("msg").asText();
                if ("connected".equalsIgnoreCase(msg)) {
                    log.info("[AlpacaWebSocketClient] Handshake received. Authenticating...");
                    sendAuth();
                } else if ("authenticated".equalsIgnoreCase(msg)) {
                    authFailed.set(false);
                    log.info("[AlpacaWebSocketClient] Authenticated successfully. Subscribing to trades...");
                    sendSubscription();
                }
            }
            case "subscription" -> log.info("[AlpacaWebSocketClient] Subscription confirmed: {}", node);
            case "error" -> {
                int code = node.path("code").asInt();
                String msg = node.path("msg").asText();
                if (code == 402 && isDefaultOrBlank(apiKey)) {
                    authFailed.set(true);
                    log.warn("[AlpacaWebSocketClient] Alpaca rejected default credentials ('{}'). "
                            + "US Equities feed is paused until valid ALPACA_API_KEY / ALPACA_API_SECRET are provided.", apiKey);
                } else {
                    log.warn("[AlpacaWebSocketClient] Server returned error [code={}]: {}", code, msg);
                }
            }
            case "t" -> processTrade(node);
            default -> log.trace("[AlpacaWebSocketClient] Unhandled message type: {}", msgType);
        }
    }

    private void sendAuth() {
        if (webSocket == null) {
            return;
        }

        try {
            String authJson = objectMapper.writeValueAsString(new AuthPayload("auth", apiKey, apiSecret));
            webSocket.sendText(authJson, true);
        } catch (Exception e) {
            log.error("[AlpacaWebSocketClient] Failed to send auth payload", e);
        }
    }

    private void sendSubscription() {
        if (webSocket == null) {
            return;
        }

        try {
            List<String> tradeSymbols = Arrays.stream(symbols.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();

            String subJson = objectMapper.writeValueAsString(new SubPayload("subscribe", tradeSymbols));
            webSocket.sendText(subJson, true);
            log.info("[AlpacaWebSocketClient] Sent trade subscriptions for: {}", tradeSymbols);
        } catch (Exception e) {
            log.error("[AlpacaWebSocketClient] Failed to send subscription payload", e);
        }
    }

    void processTrade(JsonNode tradeNode) {
        try {
            String symbol = tradeNode.path("S").asText();
            double priceDouble = tradeNode.path("p").asDouble();
            double size = tradeNode.path("s").asDouble();
            String timestampStr = tradeNode.path("t").asText();

            long timestampMs;
            if (timestampStr != null && !timestampStr.isBlank()) {
                timestampMs = Instant.parse(timestampStr).toEpochMilli();
            } else {
                timestampMs = System.currentTimeMillis();
            }

            long priceFixed = Math.round(priceDouble * RealtimeMarketDataService.PRICE_SCALE);
            MarketTick tick = new MarketTick(symbol, timestampMs, priceFixed, size);
            marketDataService.ingest(tick);

            log.info("[AlpacaWebSocketClient] Live Trade: {} @ ${} (shares={})", symbol, priceDouble, size);
        } catch (Exception e) {
            log.debug("[AlpacaWebSocketClient] Error parsing trade tick: {}", e.getMessage());
        }
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        if (authFailed.get()) {
            log.info("[AlpacaWebSocketClient] WebSocket closed following authentication rejection. Waiting for valid credentials.");
        } else {
            log.warn("[AlpacaWebSocketClient] WebSocket closed [code={}]: {}. Reconnecting in 10s...", statusCode, reason);
            scheduleReconnect(10);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        if (!authFailed.get()) {
            log.warn("[AlpacaWebSocketClient] WebSocket error: {}. Reconnecting...", error.getMessage());
            scheduleReconnect(10);
        }
    }

    @PreDestroy
    public void stop() {
        isRunning.set(false);
        reconnectScheduler.shutdownNow();
        if (webSocket != null) {
            webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "Application shutting down");
        }
        log.info("[AlpacaWebSocketClient] Stopped.");
    }

    private record AuthPayload(String action, String key, String secret) {}
    private record SubPayload(String action, List<String> trades) {}
}
