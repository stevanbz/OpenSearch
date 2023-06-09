/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry.meters;

import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import org.opensearch.performanceanalyzer.commons.metrics.AllMetrics;
import org.opensearch.performanceanalyzer.commons.metrics.AllMetrics.OSMetrics;
import org.opensearch.tracing.opentelemetry.OpenTelemetryService;

public class DiskTraceOperationMeters {

    public static DoubleHistogram readThroughputBps;
    public static DoubleHistogram writeThroughputBps;
    public static DoubleHistogram totalThroughputBps;
    public static DoubleHistogram readSyscallRate;
    public static DoubleHistogram writeSyscallRate;
    public static DoubleHistogram totalSyscallRate;
    public static DoubleHistogram pageCacheReadThroughputBps;
    public static DoubleHistogram pageCacheWriteThroughputBps;
    public static DoubleHistogram pageCacheTotalThroughputBps;
    public static DoubleHistogram cpuUtilization;
    public static DoubleHistogram pagingMajFltRate;
    public static DoubleHistogram pagingMinFltRate;
    public static DoubleHistogram pagingRss;
    public static DoubleHistogram schedRunTime;
    public static DoubleHistogram schedWaitTime;
    public static DoubleHistogram schedCtxRate;
    public static DoubleHistogram heapAllocRate;
    public static LongHistogram blockedCount;
    public static DoubleHistogram blockedTime;
    public static LongHistogram waitedCount;
    public static DoubleHistogram waitedTime;

    static  {
        Meter meter = getMeter();
        readThroughputBps = meter.histogramBuilder("ReadThroughputBps").build();
        writeThroughputBps = meter.histogramBuilder("WriteThroughputBps").build();
        totalThroughputBps = meter.histogramBuilder("TotalThroughputBps").build();
        readSyscallRate = meter.histogramBuilder("ReadSyscallRate").build();
        writeSyscallRate = meter.histogramBuilder("WriteSyscallRate").build();
        totalSyscallRate = meter.histogramBuilder("TotalSyscallRate").build();
        pageCacheReadThroughputBps = meter.histogramBuilder("PageCacheReadThroughputBps").build();
        pageCacheWriteThroughputBps = meter.histogramBuilder("PageCacheWriteThroughputBps").build();
        pageCacheTotalThroughputBps = meter.histogramBuilder("PageCacheTotalThroughputBps").build();

        cpuUtilization = meter.histogramBuilder("CPUUtilizationPA").build();
        pagingMajFltRate = meter.histogramBuilder(AllMetrics.OSMetrics.PAGING_MAJ_FLT_RATE.toString()).build();
        pagingMinFltRate = meter.histogramBuilder(AllMetrics.OSMetrics.PAGING_MIN_FLT_RATE.toString()).build();
        pagingRss = meter.histogramBuilder(AllMetrics.OSMetrics.PAGING_RSS.toString()).build();

        schedRunTime =  meter.histogramBuilder(AllMetrics.OSMetrics.SCHED_RUNTIME.toString()).build();
        schedWaitTime =  meter.histogramBuilder(AllMetrics.OSMetrics.SCHED_WAITTIME.toString()).build();
        schedCtxRate =  meter.histogramBuilder(AllMetrics.OSMetrics.SCHED_CTX_RATE.toString()).build();

        heapAllocRate = meter.histogramBuilder(AllMetrics.OSMetrics.HEAP_ALLOC_RATE.name()).build();
        blockedCount = meter.histogramBuilder(OSMetrics.THREAD_BLOCKED_EVENT.toString()).ofLongs().build();
        blockedTime = meter.histogramBuilder(OSMetrics.THREAD_WAITED_TIME.toString()).build();
        waitedCount = meter.histogramBuilder(AllMetrics.OSMetrics.THREAD_WAITED_EVENT.toString()).ofLongs().build();
        waitedTime = meter.histogramBuilder(AllMetrics.OSMetrics.THREAD_WAITED_TIME.toString()).build();
    }

    public static void reset() {
        readThroughputBps.record(0);
        writeThroughputBps.record(0);
        totalThroughputBps.record(0);
        readSyscallRate.record(0);
        writeSyscallRate.record(0);
        totalSyscallRate.record(0);
        pageCacheReadThroughputBps.record(0);
        pageCacheWriteThroughputBps.record(0);
        pageCacheTotalThroughputBps.record(0);

        cpuUtilization.record(0);
        pagingMajFltRate.record(0);
        pagingMinFltRate.record(0);
        pagingRss.record(0);

        schedRunTime.record(0);
        schedWaitTime.record(0);
        schedCtxRate.record(0);

        heapAllocRate.record(0);

        blockedCount.record(0);
        blockedTime.record(0);
        waitedCount.record(0);
        waitedTime.record(0);
    }

    public static Meter getMeter() {
        return OpenTelemetryService.meter;
    }
}
