/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry;

import static org.opensearch.performanceanalyzer.commons.os.metrics.CPUMetricsCalculator.calculateThreadCpuPagingActivity;
import static org.opensearch.performanceanalyzer.commons.os.metrics.DiskIOMetricsCalculator.calculateIOMetrics;
import static org.opensearch.performanceanalyzer.commons.os.metrics.SchedMetricsCalculator.calculateThreadSchedLatency;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import org.opensearch.performanceanalyzer.commons.collectors.OSMetricsCollector;
import org.opensearch.performanceanalyzer.commons.jvm.ThreadList;
import org.opensearch.performanceanalyzer.commons.metrics.AllMetrics;
import org.opensearch.performanceanalyzer.commons.metrics.AllMetrics.OSMetrics;
import org.opensearch.performanceanalyzer.commons.os.metrics.CPUMetrics;
import org.opensearch.performanceanalyzer.commons.os.metrics.IOMetrics;
import org.opensearch.performanceanalyzer.commons.os.metrics.SchedMetrics;
import org.opensearch.tracing.TaskEventListener;
import org.opensearch.tracing.opentelemetry.meters.DiskTraceOperationMeters;

public class DiskStatsEventListener implements TaskEventListener {
    public static DiskStatsEventListener INSTANCE = new DiskStatsEventListener();

    static class SupportedMeasurement {
        public long startTime;
        public long endTime;
        public String nativeThreadId;
        private Map<String, Object> cpuThreadDetailsMap;
        private Map<String, Long> diskIOThreadDetailsMap;
        private Map<String, Object> schedThreadDetailsMap;
        private ThreadList.ThreadState threadState;

        private SupportedMeasurement() {
            cpuThreadDetailsMap = new HashMap<>();
            diskIOThreadDetailsMap = new HashMap<>();
            schedThreadDetailsMap = new HashMap<>();
        }

        private SupportedMeasurement(Thread t, boolean threadContentionEnabled) {
            startTime = System.currentTimeMillis();
            long jTid = t.getId();

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

            this.nativeThreadId = String.valueOf(nativeThreadID);

            this.cpuThreadDetailsMap = OpenTelemetryService.cpuObserver.observe(nativeThreadId);
            this.diskIOThreadDetailsMap = OpenTelemetryService.ioObserver.observe(nativeThreadId);
            this.schedThreadDetailsMap = OpenTelemetryService.schedObserver.observe(nativeThreadId);
        }

        private static void resetHistogram() {
            DiskTraceOperationMeters.reset();
        }

        public void endRecording() {
            endTime = System.currentTimeMillis();
            Baggage baggage = Baggage.current();
            AttributesBuilder attributesBuilder = Attributes.builder();
            baggage.forEach((key, baggageEntry) -> attributesBuilder.put(key, baggageEntry.getValue()));
            if (threadState != null) {
                attributesBuilder.put(
                    AttributeKey.stringKey(
                        OSMetricsCollector.MetaDataFields.threadName.toString()),
                    threadState.threadName);

            }
            Attributes attributes = attributesBuilder.build();

            // Get resource metrics for thread
            Map<String, Object> endCpuDetails = OpenTelemetryService.cpuObserver.observe(nativeThreadId);
            Map<String, Long> endDiskIODetails = OpenTelemetryService.ioObserver.observe(nativeThreadId);
            Map<String, Object> endSchedODetails = OpenTelemetryService.schedObserver.observe(nativeThreadId);

            if (!endCpuDetails.isEmpty()) {
                CPUMetrics cpuMetrics = calculateThreadCpuPagingActivity(
                    endTime,
                    startTime,
                    endCpuDetails,
                    cpuThreadDetailsMap
                );

                if (cpuMetrics != null) {
                    DiskTraceOperationMeters.cpuUtilization.record(cpuMetrics.cpuUtilization, attributes);

                    DiskTraceOperationMeters.pagingMajFltRate.record(cpuMetrics.majorFault, attributes);

                    DiskTraceOperationMeters.pagingMinFltRate.record(cpuMetrics.minorFault, attributes);

                    DiskTraceOperationMeters.pagingRss.record(cpuMetrics.residentSetSize, attributes);
                }
            }

            if (!endDiskIODetails.isEmpty()) {
                IOMetrics diskIOMetrics = calculateIOMetrics(
                    endTime,
                    startTime,
                    endDiskIODetails,
                    diskIOThreadDetailsMap);

                if (diskIOMetrics != null) {
                    DiskTraceOperationMeters.readThroughputBps.record(diskIOMetrics.avgReadThroughputBps, attributes);

                    DiskTraceOperationMeters.writeThroughputBps.record(diskIOMetrics.avgWriteThroughputBps, attributes);

                    DiskTraceOperationMeters.totalThroughputBps.record(diskIOMetrics.avgTotalThroughputBps, attributes);

                    DiskTraceOperationMeters.readSyscallRate.record(diskIOMetrics.avgReadSyscallRate, attributes);

                    DiskTraceOperationMeters.writeSyscallRate.record(diskIOMetrics.avgWriteSyscallRate, attributes);

                    DiskTraceOperationMeters.totalSyscallRate.record(diskIOMetrics.avgTotalSyscallRate, attributes);

                    DiskTraceOperationMeters.pageCacheReadThroughputBps.record(diskIOMetrics.avgPageCacheReadThroughputBps, attributes);

                    DiskTraceOperationMeters.pageCacheWriteThroughputBps.record(diskIOMetrics.avgPageCacheWriteThroughputBps, attributes);

                    DiskTraceOperationMeters.pageCacheTotalThroughputBps.record(diskIOMetrics.avgPageCacheTotalThroughputBps, attributes);
                }
            }

            if (!endSchedODetails.isEmpty()) {
                SchedMetrics schedMetrics = calculateThreadSchedLatency(
                    endTime,
                    startTime,
                    endSchedODetails,
                    schedThreadDetailsMap
                );

                if (schedMetrics != null) {
                    DiskTraceOperationMeters.schedRunTime.record(schedMetrics.avgRuntime, attributes);

                    DiskTraceOperationMeters.schedWaitTime.record(schedMetrics.avgWaittime, attributes);

                    DiskTraceOperationMeters.schedCtxRate.record(schedMetrics.contextSwitchRate, attributes);
                }
            }
            if (threadState != null) {
                DiskTraceOperationMeters.heapAllocRate.record(threadState.heapAllocRate, attributes);

                DiskTraceOperationMeters.blockedCount.record(threadState.blockedCount, attributes);

                DiskTraceOperationMeters.blockedTime.record(threadState.blockedTime, attributes);

                DiskTraceOperationMeters.waitedCount.record(threadState.waitedCount, attributes);

                DiskTraceOperationMeters.waitedTime.record(threadState.waitedTime, attributes);
            }
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
