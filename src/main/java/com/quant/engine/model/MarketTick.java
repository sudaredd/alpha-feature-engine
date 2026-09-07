package com.quant.engine.model;

/**
 * Represents a raw market tick event.
 *
 * @param symbol      Ticker symbol (e.g., "PLTR", "BTCUSDT")
 * @param timestampMs Epoch timestamp in milliseconds
 * @param price       Fixed-point scaled price (e.g., $150.25 -> 1502500L, scaled by 10^4)
 * @param volume      Number of shares/contracts/tokens traded (supports fractional amounts)
 */
public record MarketTick(
        String symbol,
        long timestampMs,
        long price,
        double volume
) {
    public MarketTick {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol cannot be null or blank");
        }
        if (price < 0) {
            throw new IllegalArgumentException("price cannot be negative: " + price);
        }
        if (volume < 0 || Double.isNaN(volume) || Double.isInfinite(volume)) {
            throw new IllegalArgumentException("volume cannot be negative or non-finite: " + volume);
        }
    }
}
