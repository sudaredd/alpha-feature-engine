package com.quant.engine.model;

import java.time.Instant;

/**
 * Aggregated OHLCV and VWAP time bar.
 *
 * @param timestamp   Bucket start time as an Instant
 * @param open        Open price in double representation
 * @param high        High price in double representation
 * @param low         Low price in double representation
 * @param close       Close price in double representation
 * @param vwap        Volume-Weighted Average Price in double representation
 * @param totalVolume Total accumulated volume across all ticks in this bar
 */
public record TimeBar(
        Instant timestamp,
        double open,
        double high,
        double low,
        double close,
        double vwap,
        double totalVolume
) {
}
