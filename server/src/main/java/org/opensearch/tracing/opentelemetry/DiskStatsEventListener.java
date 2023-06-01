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

        public SupportedMeasurement(Thread t, boolean threadContentionEnabled) {
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

            Map<String, Object> mapOfValues = new HashMap<>();

            // Get resource metrics for thread
            Map<String, Object> endCpuDetails = OpenTelemetryService.cpuObserver.observe(nativeThreadId);
            Map<String, Long> endDiskIODetails = OpenTelemetryService.ioObserver.observe(nativeThreadId);
            Map<String, Object> endSchedODetails = OpenTelemetryService.schedObserver.observe(nativeThreadId);

            if (endCpuDetails.isEmpty() == false) {
                CPUMetrics cpuMetrics = calculateThreadCpuPagingActivity(
                    endTime,
                    startTime,
                    endCpuDetails,
                    cpuThreadDetailsMap
                );

                if (cpuMetrics != null) {
                    DiskTraceOperationMeters.cpuUtilization.record(cpuMetrics.cpuUtilization, attributes);
                    mapOfValues.put(OSMetrics.CPU_UTILIZATION.toString(), cpuMetrics.cpuUtilization);

                    DiskTraceOperationMeters.pagingMajFltRate.record(cpuMetrics.majorFault, attributes);
                    mapOfValues.put(OSMetrics.PAGING_MAJ_FLT_RATE.toString(), cpuMetrics.majorFault);

                    DiskTraceOperationMeters.pagingMinFltRate.record(cpuMetrics.minorFault, attributes);
                    mapOfValues.put(OSMetrics.PAGING_MIN_FLT_RATE.toString(), cpuMetrics.minorFault);

                    DiskTraceOperationMeters.pagingRss.record(cpuMetrics.residentSetSize, attributes);
                    mapOfValues.put(OSMetrics.PAGING_RSS.toString(), cpuMetrics.residentSetSize);
                }
            }

            if (endDiskIODetails.isEmpty() == false) {
                IOMetrics diskIOMetrics = calculateIOMetrics(
                    endTime,
                    startTime,
                    endDiskIODetails,
                    diskIOThreadDetailsMap);

                if (diskIOMetrics != null) {
                    DiskTraceOperationMeters.readThroughputBps.record(diskIOMetrics.avgReadThroughputBps, attributes);
                    mapOfValues.put("ReadThroughputBps", diskIOMetrics.avgReadThroughputBps);

                    DiskTraceOperationMeters.writeThroughputBps.record(diskIOMetrics.avgWriteThroughputBps, attributes);
                    mapOfValues.put("WriteThroughputBps", diskIOMetrics.avgWriteThroughputBps);

                    DiskTraceOperationMeters.totalThroughputBps.record(diskIOMetrics.avgTotalThroughputBps, attributes);
                    mapOfValues.put("TotalThroughputBps", diskIOMetrics.avgTotalThroughputBps);

                    DiskTraceOperationMeters.readSyscallRate.record(diskIOMetrics.avgReadSyscallRate, attributes);
                    mapOfValues.put("ReadSyscallRate", diskIOMetrics.avgReadSyscallRate);

                    DiskTraceOperationMeters.writeSyscallRate.record(diskIOMetrics.avgWriteSyscallRate, attributes);
                    mapOfValues.put("WriteSyscallRate", diskIOMetrics.avgWriteSyscallRate);

                    DiskTraceOperationMeters.totalSyscallRate.record(diskIOMetrics.avgTotalSyscallRate, attributes);
                    mapOfValues.put("TotalSyscallRate", diskIOMetrics.avgTotalSyscallRate);

                    DiskTraceOperationMeters.pageCacheReadThroughputBps.record(diskIOMetrics.avgPageCacheReadThroughputBps, attributes);
                    mapOfValues.put("PageCacheReadThroughputBps", diskIOMetrics.avgPageCacheReadThroughputBps);

                    DiskTraceOperationMeters.pageCacheWriteThroughputBps.record(diskIOMetrics.avgPageCacheWriteThroughputBps, attributes);
                    mapOfValues.put("PageCacheWriteThroughputBps", diskIOMetrics.avgPageCacheWriteThroughputBps);

                    DiskTraceOperationMeters.pageCacheTotalThroughputBps.record(diskIOMetrics.avgPageCacheTotalThroughputBps, attributes);
                    mapOfValues.put("PageCacheTotalThroughputBps", diskIOMetrics.avgPageCacheTotalThroughputBps);
                }
            }

            if (endSchedODetails.isEmpty() == false) {
                SchedMetrics schedMetrics = calculateThreadSchedLatency(
                    endTime,
                    startTime,
                    endSchedODetails,
                    schedThreadDetailsMap
                );

                if (schedMetrics != null) {
                    DiskTraceOperationMeters.schedRunTime.record(schedMetrics.avgRuntime, attributes);
                    mapOfValues.put(AllMetrics.OSMetrics.SCHED_RUNTIME.toString(), schedMetrics.avgRuntime);

                    DiskTraceOperationMeters.schedWaitTime.record(schedMetrics.avgWaittime, attributes);
                    mapOfValues.put(OSMetrics.SCHED_WAITTIME.toString(), schedMetrics.avgWaittime);

                    DiskTraceOperationMeters.schedCtxRate.record(schedMetrics.contextSwitchRate, attributes);
                    mapOfValues.put(OSMetrics.SCHED_CTX_RATE.toString(), schedMetrics.contextSwitchRate);
                }
            }
            if (threadState != null) {
                DiskTraceOperationMeters.heapAllocRate.record(threadState.heapAllocRate, attributes);
                mapOfValues.put(AllMetrics.OSMetrics.HEAP_ALLOC_RATE.name(), threadState.heapAllocRate);

                DiskTraceOperationMeters.blockedCount.add(threadState.blockedCount, attributes);
                mapOfValues.put(OSMetrics.THREAD_BLOCKED_EVENT.name(), threadState.blockedCount);

                DiskTraceOperationMeters.blockedTime.add(threadState.blockedTime, attributes);
                mapOfValues.put(OSMetrics.THREAD_BLOCKED_TIME.name(), threadState.blockedTime);

                DiskTraceOperationMeters.waitedCount.add(threadState.waitedCount, attributes);
                mapOfValues.put(AllMetrics.OSMetrics.THREAD_WAITED_EVENT.toString(), threadState.waitedCount);

                DiskTraceOperationMeters.waitedTime.add(threadState.waitedTime, attributes);
                mapOfValues.put(AllMetrics.OSMetrics.THREAD_WAITED_TIME.toString(), threadState.waitedTime);
            }

            mapOfValues.forEach(
                (k, v) -> {
                    if (v instanceof Double) {
                        System.out.print(k + ":" + v + ", ");
                    } else if (v instanceof Long) {
                        System.out.print("Long=" + k + ":" + v + ", ");

                    } else {
                        System.out.print("ND=" + k + ":" + v + ", ");
                    }
                });
            System.out.println("\nDone");
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
