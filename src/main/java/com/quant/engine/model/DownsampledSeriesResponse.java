package com.quant.engine.model;

import java.util.List;

/**
 * Standard response payload containing aggregated downsampled time bars for an MCP tool query.
 *
 * @param symbol            Ticker symbol queried
 * @param resolutionSeconds Downsampling bar resolution in seconds
 * @param bars              Aggregated TimeBar objects
 */
public record DownsampledSeriesResponse(
        String symbol,
        int resolutionSeconds,
        List<TimeBar> bars
) {
    public DownsampledSeriesResponse {
        bars = bars == null ? List.of() : List.copyOf(bars);
    }
}
