package com.quant.engine.model;

/**
 * Result payload returned when a trade order is submitted to the execution engine.
 *
 * @param orderId     Unique identifier of the executed order
 * @param symbol      Ticker symbol (e.g., "PLTR")
 * @param side        Order side ("BUY" or "SELL")
 * @param quantity    Number of shares/contracts executed
 * @param status      Current execution status (e.g., "FILLED")
 * @param timestampMs Epoch millisecond execution timestamp
 */
public record OrderExecutionResponse(
        String orderId,
        String symbol,
        String side,
        long quantity,
        String status,
        long timestampMs
) {
}
