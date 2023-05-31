/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry.meters;

import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import org.opensearch.tracing.opentelemetry.OpenTelemetryService;

public class TraceOperationMeters {
    public static DoubleHistogram cpuTime;
    public static DoubleHistogram cpuUtilization;
    public static DoubleHistogram heapAllocatedBytes;
    public static LongHistogram blockedCount;
    public static LongHistogram waitedCount;
    public static DoubleHistogram blockedTime;
    public static DoubleHistogram waitedTime;
    public static DoubleHistogram elapsedTime;


    static  {
        Meter meter = getMeter();
        cpuTime = meter.histogramBuilder("CPUTime").build();
        cpuUtilization = meter.histogramBuilder("CPUUtilization").build();
        heapAllocatedBytes = meter.histogramBuilder("HeapAllocatedBytes").build();
        blockedCount = meter.histogramBuilder("BlockedCount").ofLongs().build();
        waitedCount = meter.histogramBuilder("WaitedCount").ofLongs().build();
        blockedTime = meter.histogramBuilder("BlockedTime").build();
        waitedTime = meter.histogramBuilder("WaitedTime").build();
        elapsedTime = meter.histogramBuilder("ElapsedTime").build();
    }

    public static Meter getMeter() {
        return OpenTelemetryService.meter;
    }
}
