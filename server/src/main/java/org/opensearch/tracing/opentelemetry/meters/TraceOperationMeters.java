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
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import org.opensearch.tracing.opentelemetry.OpenTelemetryService;

public class TraceOperationMeters {
    public static DoubleHistogram cpuTime;
    public static ObservableDoubleMeasurement cpuUtilization;
    public static ObservableDoubleMeasurement heapAllocatedBytes;
    public static LongCounter blockedCount;
    public static LongCounter waitedCount;
    public static DoubleHistogram blockedTime;
    public static DoubleHistogram waitedTime;
    public static DoubleHistogram elapsedTime;


    static  {
        Meter meter = getMeter();
        cpuTime = meter.histogramBuilder("CPUTime").build();
        cpuUtilization = meter.gaugeBuilder("CPUUtilization").buildObserver();
        heapAllocatedBytes = meter.gaugeBuilder("HeapAllocatedBytes").buildObserver();
        blockedCount = meter.counterBuilder("BlockedCount").build();
        waitedCount = meter.counterBuilder("WaitedCount").build();
        blockedTime = meter.histogramBuilder("BlockedTime").build();
        waitedTime = meter.histogramBuilder("WaitedTime").build();
        elapsedTime = meter.histogramBuilder("ElapsedTime").build();
    }

    public static Meter getMeter() {
        return OpenTelemetryService.meter;
    }
}
