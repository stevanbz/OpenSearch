/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import org.opensearch.performanceanalyzer.commons.jvm.ThreadList;
import org.opensearch.performanceanalyzer.commons.os.ThreadDiskIO;
import org.opensearch.tracing.TaskEventListener;
import org.opensearch.tracing.opentelemetry.meters.DiskTraceOperationMeters;

public class DiskStatsEventListener implements TaskEventListener {
    public static DiskStatsEventListener INSTANCE = new DiskStatsEventListener();

    static class SupportedMeasurement {
        public long startTime;

        public long endTime;
        // Disk IO metrics
        public double readThroughputBps;

        public double writeThroughputBps;

        public double totalThroughputBps;

        public double readSyscallRate;

        public double writeSyscallRate;

        public double totalSyscallRate;

        public double pageCacheReadThroughputBps;

        public double pageCacheWriteThroughputBps;

        public double pageCacheTotalThroughputBps;
        // CPU metrics
        public double cpuUtilization;

        public double pagingMajFltRate;

        public double pagingMinFltRate;

        public double pagingRss;
        // Sched metrics
        public double schedRunTime;
        public double schedWaitTime;
        public double schedCtxRate;
        // Thread info
        public String threadName;
        public double heapAllocRate;
        public long blockedCount;
        public long blockedTime;
        public long waitedCount;
        public long waitedTime;

        public Thread t;

        public long duration;
        private final boolean threadContentionEnabled;

        public SupportedMeasurement(Thread t, boolean threadContentionEnabled) {
            startTime = System.currentTimeMillis();

            this.threadContentionEnabled = threadContentionEnabled;
            long jTid = t.getId();

            ThreadList.ThreadState threadState;
            long nativeThreadID = OpenTelemetryService.threadIdUtil.getNativeThreadId(t.getId());
            if (nativeThreadID == -1) {
                // TODO - make it async
                ThreadList.getNativeTidMap(threadContentionEnabled);
                nativeThreadID = OpenTelemetryService.threadIdUtil.getNativeThreadId(jTid);

                if (nativeThreadID == -1) {
                    return;
                }
                threadState = ThreadList.getThreadState(jTid);
            } else {
                threadState = ThreadList.getThreadState(jTid);
            }
            OpenTelemetryService.threadCPUPagingActivityGenerator.addSample(String.valueOf(nativeThreadID));

            OpenTelemetryService.schedMetricsGenerator.addSample(String.valueOf(nativeThreadID));

            ThreadDiskIO.addSample(String.valueOf(nativeThreadID));

            if (OpenTelemetryService.diskIOMetricsGenerator.hasDiskIOMetrics(String.valueOf(nativeThreadID))) {
                readThroughputBps =
                    OpenTelemetryService.diskIOMetricsGenerator.getAvgReadThroughputBps(
                        String.valueOf(nativeThreadID));
                writeThroughputBps =
                    OpenTelemetryService.diskIOMetricsGenerator.getAvgWriteThroughputBps(
                        String.valueOf(nativeThreadID));
                totalThroughputBps =
                    OpenTelemetryService.diskIOMetricsGenerator.getAvgTotalThroughputBps(
                        String.valueOf(nativeThreadID));
                readSyscallRate =
                    OpenTelemetryService.diskIOMetricsGenerator.getAvgReadSyscallRate(
                        String.valueOf(nativeThreadID));
                writeSyscallRate =
                    OpenTelemetryService.diskIOMetricsGenerator.getAvgWriteSyscallRate(
                        String.valueOf(nativeThreadID));
                totalSyscallRate =
                    OpenTelemetryService.diskIOMetricsGenerator.getAvgTotalSyscallRate(
                        String.valueOf(nativeThreadID));
                pageCacheReadThroughputBps =
                    OpenTelemetryService.diskIOMetricsGenerator.getAvgPageCacheReadThroughputBps(
                        String.valueOf(nativeThreadID));
                pageCacheWriteThroughputBps =
                    OpenTelemetryService.diskIOMetricsGenerator.getAvgPageCacheWriteThroughputBps(
                        String.valueOf(nativeThreadID));
                pageCacheTotalThroughputBps =
                    OpenTelemetryService.diskIOMetricsGenerator.getAvgPageCacheTotalThroughputBps(
                        String.valueOf(nativeThreadID));
            }

            if (OpenTelemetryService.threadCPUPagingActivityGenerator.hasPagingActivity(
                String.valueOf(nativeThreadID))) {
                cpuUtilization =
                    OpenTelemetryService.threadCPUPagingActivityGenerator.getCPUUtilization(
                        String.valueOf(nativeThreadID));
                pagingMajFltRate =
                    OpenTelemetryService.threadCPUPagingActivityGenerator.getMajorFault(
                        String.valueOf(nativeThreadID));
                pagingMinFltRate =
                    OpenTelemetryService.threadCPUPagingActivityGenerator.getMinorFault(
                        String.valueOf(nativeThreadID));
                pagingRss =
                    OpenTelemetryService.threadCPUPagingActivityGenerator.getResidentSetSize(
                        String.valueOf(nativeThreadID));
            }
            if (OpenTelemetryService.schedMetricsGenerator.hasSchedMetrics(String.valueOf(nativeThreadID))) {
                schedRunTime =
                    OpenTelemetryService.schedMetricsGenerator.getAvgRuntime(String.valueOf(nativeThreadID));
                schedWaitTime =
                    OpenTelemetryService.schedMetricsGenerator.getAvgWaittime(String.valueOf(nativeThreadID));
                schedCtxRate =
                    OpenTelemetryService.schedMetricsGenerator.getContextSwitchRate(String.valueOf(nativeThreadID));
            }
            if (threadState != null) {
                threadName = threadState.threadName;
                heapAllocRate = threadState.heapAllocRate;
                blockedCount = threadState.blockedCount;
                blockedTime = threadState.blockedTime;
                waitedCount = threadState.waitedCount;
                waitedTime = threadState.waitedTime;
            }
        }

        private static void resetHistogram() {
            DiskTraceOperationMeters.reset();
        }

        public void endRecording() {
            endTime = System.currentTimeMillis();
            Baggage baggage = Baggage.current();
            AttributesBuilder attributesBuilder = Attributes.builder();
            baggage.forEach((key, baggageEntry) -> {
                attributesBuilder.put(key, baggageEntry.getValue());
            });
            // TODO - check with Rishabh should we do a calculation before and after
            Attributes attributes = attributesBuilder.build();
            DiskTraceOperationMeters.readThroughputBps.record(0, attributes);
            DiskTraceOperationMeters.writeThroughputBps.record(0, attributes);
            DiskTraceOperationMeters.totalThroughputBps.record(0, attributes);
            DiskTraceOperationMeters.readSyscallRate.record(0, attributes);
            DiskTraceOperationMeters.writeSyscallRate.record(0, attributes);
            DiskTraceOperationMeters.totalSyscallRate.record(0, attributes);
            DiskTraceOperationMeters.pageCacheReadThroughputBps.record(0, attributes);
            DiskTraceOperationMeters.pageCacheWriteThroughputBps.record(0, attributes);
            DiskTraceOperationMeters.pageCacheTotalThroughputBps.record(0, attributes);

            DiskTraceOperationMeters.cpuUtilization.record(0, attributes);
            DiskTraceOperationMeters.pagingMajFltRate.record(0, attributes);
            DiskTraceOperationMeters.pagingMinFltRate.record(0, attributes);
            DiskTraceOperationMeters.pagingRss.record(0, attributes);

            DiskTraceOperationMeters.schedRunTime.record(0, attributes);
            DiskTraceOperationMeters.schedWaitTime.record(0, attributes);
            DiskTraceOperationMeters.schedCtxRate.record(0, attributes);

            DiskTraceOperationMeters.heapAllocRate.record(0, attributes);

            DiskTraceOperationMeters.blockedCount.add(0, attributes);
            DiskTraceOperationMeters.blockedTime.add(0, attributes);
            DiskTraceOperationMeters.waitedCount.add(0, attributes);
            DiskTraceOperationMeters.waitedTime.add(0, attributes);

            this.duration = endTime - startTime;
            // meter.gaugeBuilder("CPUUtilization").buildWithCallback(callback -> cpuUtilization = callback);
            resetHistogram();
        }
    }

    private static final Map<Long, Stack<SupportedMeasurement>> threadDiskIOUsage = new HashMap<>();

    @Override
    public void onStart(String operationName, String eventName, Thread t) {
        threadDiskIOUsage.computeIfAbsent(t.getId(), k -> new Stack<>());
        threadDiskIOUsage.get(t.getId()).add(new SupportedMeasurement(t, true));
    }

    @Override
    public void onEnd(String operationName, String eventName, Thread t) {
        SupportedMeasurement threadMeasurements = threadDiskIOUsage.get(t.getId()).peek();
        threadMeasurements.endRecording();
        threadDiskIOUsage.get(t.getId()).pop();
    }

    @Override
    public boolean isApplicable(String operationName, String eventName) {
        return true;
    }
}
