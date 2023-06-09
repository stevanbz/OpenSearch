/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry.meters;

import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import org.opensearch.performanceanalyzer.commons.metrics.AllMetrics.DiskValue;
import org.opensearch.tracing.opentelemetry.OpenTelemetryService;

public class DiskStatsOperationMeters {
    public static DoubleHistogram utilization;
    public static DoubleHistogram await;
    public static DoubleHistogram serviceRate;

    static {
        Meter meter = getMeter();
        utilization = meter.histogramBuilder(DiskValue.DISK_UTILIZATION.name()).build();
        await = meter.histogramBuilder(DiskValue.DISK_WAITTIME.name()).build();
        serviceRate = meter.histogramBuilder(DiskValue.DISK_SERVICE_RATE.name()).build();
    }

    public static void reset() {
        utilization.record(0);
        await.record(0);
        serviceRate.record(0);
    }

    public static Meter getMeter() {
        return OpenTelemetryService.periodicalMeter;
    }
}
