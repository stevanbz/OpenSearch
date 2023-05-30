/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import org.opensearch.performanceanalyzer.commons.collectors.OSMetricsCollector;
import org.opensearch.performanceanalyzer.commons.jvm.ThreadList;
import org.opensearch.performanceanalyzer.commons.os.ThreadCPUMetricsGenerator;
import org.opensearch.performanceanalyzer.commons.os.ThreadDiskIO;
import org.opensearch.performanceanalyzer.commons.os.ThreadDiskIO.IOMetrics;
import org.opensearch.performanceanalyzer.commons.os.ThreadDiskIOMetricGenerator;
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

        public String nativeThreadId;

        public long duration;
        private final boolean threadContentionEnabled;

        private Map<String, Object> cpuThreadStartDetails;
        private Map<String, Long> diskIOThreadStartDetails;
        private Map<String, Object> schedThreadStartDetails;

        private ThreadList.ThreadState threadState;

        private SupportedMeasurement() {
            threadContentionEnabled = false;
            cpuThreadStartDetails = new HashMap<>();
            diskIOThreadStartDetails = new HashMap<>();
            schedThreadStartDetails = new HashMap<>();
        }

        public SupportedMeasurement(Thread t, boolean threadContentionEnabled) {
            startTime = System.currentTimeMillis();

            this.threadContentionEnabled = threadContentionEnabled;
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

            this.cpuThreadStartDetails = OpenTelemetryService.cpuObserver.observe(nativeThreadId);
            this.diskIOThreadStartDetails = (Map)OpenTelemetryService.ioObserver.observe(nativeThreadId);
            this.schedThreadStartDetails = OpenTelemetryService.schedObserver.observe(nativeThreadId);

//            OpenTelemetryService.threadCPUPagingActivityGenerator.addSample(String.valueOf(nativeThreadID));
//
//            OpenTelemetryService.schedMetricsGenerator.addSample(String.valueOf(nativeThreadID));
//
//            ThreadDiskIO.addSample(String.valueOf(nativeThreadID));
//
//            if (OpenTelemetryService.diskIOMetricsGenerator.hasDiskIOMetrics(String.valueOf(nativeThreadID))) {
//                readThroughputBps =
//                    OpenTelemetryService.diskIOMetricsGenerator.getAvgReadThroughputBps(
//                        String.valueOf(nativeThreadID));
//                writeThroughputBps =
//                    OpenTelemetryService.diskIOMetricsGenerator.getAvgWriteThroughputBps(
//                        String.valueOf(nativeThreadID));
//                totalThroughputBps =
//                    OpenTelemetryService.diskIOMetricsGenerator.getAvgTotalThroughputBps(
//                        String.valueOf(nativeThreadID));
//                readSyscallRate =
//                    OpenTelemetryService.diskIOMetricsGenerator.getAvgReadSyscallRate(
//                        String.valueOf(nativeThreadID));
//                writeSyscallRate =
//                    OpenTelemetryService.diskIOMetricsGenerator.getAvgWriteSyscallRate(
//                        String.valueOf(nativeThreadID));
//                totalSyscallRate =
//                    OpenTelemetryService.diskIOMetricsGenerator.getAvgTotalSyscallRate(
//                        String.valueOf(nativeThreadID));
//                pageCacheReadThroughputBps =
//                    OpenTelemetryService.diskIOMetricsGenerator.getAvgPageCacheReadThroughputBps(
//                        String.valueOf(nativeThreadID));
//                pageCacheWriteThroughputBps =
//                    OpenTelemetryService.diskIOMetricsGenerator.getAvgPageCacheWriteThroughputBps(
//                        String.valueOf(nativeThreadID));
//                pageCacheTotalThroughputBps =
//                    OpenTelemetryService.diskIOMetricsGenerator.getAvgPageCacheTotalThroughputBps(
//                        String.valueOf(nativeThreadID));
//            }
//
//            if (OpenTelemetryService.threadCPUPagingActivityGenerator.hasPagingActivity(
//                String.valueOf(nativeThreadID))) {
//                cpuUtilization =
//                    OpenTelemetryService.threadCPUPagingActivityGenerator.getCPUUtilization(
//                        String.valueOf(nativeThreadID));
//                pagingMajFltRate =
//                    OpenTelemetryService.threadCPUPagingActivityGenerator.getMajorFault(
//                        String.valueOf(nativeThreadID));
//                pagingMinFltRate =
//                    OpenTelemetryService.threadCPUPagingActivityGenerator.getMinorFault(
//                        String.valueOf(nativeThreadID));
//                pagingRss =
//                    OpenTelemetryService.threadCPUPagingActivityGenerator.getResidentSetSize(
//                        String.valueOf(nativeThreadID));
//            }
//            if (OpenTelemetryService.schedMetricsGenerator.hasSchedMetrics(String.valueOf(nativeThreadID))) {
//                schedRunTime =
//                    OpenTelemetryService.schedMetricsGenerator.getAvgRuntime(String.valueOf(nativeThreadID));
//                schedWaitTime =
//                    OpenTelemetryService.schedMetricsGenerator.getAvgWaittime(String.valueOf(nativeThreadID));
//                schedCtxRate =
//                    OpenTelemetryService.schedMetricsGenerator.getContextSwitchRate(String.valueOf(nativeThreadID));
//            }
//            if (threadState != null) {
//                threadName = threadState.threadName;
//                heapAllocRate = threadState.heapAllocRate;
//                blockedCount = threadState.blockedCount;
//                blockedTime = threadState.blockedTime;
//                waitedCount = threadState.waitedCount;
//                waitedTime = threadState.waitedTime;
//            }
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

            Map<String, Object> endCpuDetails = OpenTelemetryService.cpuObserver.observe(nativeThreadId);
            if (endCpuDetails.isEmpty() == false) {
                double cpuUtilization = ThreadCPUMetricsGenerator.calculateThreadCPUDetails(
                    endCpuDetails,
                    cpuThreadStartDetails,
                    startTime,
                    endTime
                );
                double majorFault = ThreadCPUMetricsGenerator.calculateMajorFault(
                    endCpuDetails,
                    cpuThreadStartDetails,
                    startTime,
                    endTime
                );
                double minorFault = ThreadCPUMetricsGenerator.calculateMinorFault(
                    endCpuDetails,
                    cpuThreadStartDetails,
                    startTime,
                    endTime
                );
                double rss = ThreadCPUMetricsGenerator.getResidentSetSize(endCpuDetails);

                DiskTraceOperationMeters.cpuUtilization.record(cpuUtilization, attributes);
                mapOfValues.put("cpuUtilization", cpuUtilization);
                DiskTraceOperationMeters.pagingMajFltRate.record(majorFault, attributes);
                mapOfValues.put("pagingMajFltRate", pagingMajFltRate);
                DiskTraceOperationMeters.pagingMinFltRate.record(minorFault, attributes);
                mapOfValues.put("pagingMinFltRate", pagingMinFltRate);
                DiskTraceOperationMeters.pagingRss.record(rss, attributes);
                mapOfValues.put("pagingRss", pagingRss);

            }
            Map<String, Long> endDiskIODetails = OpenTelemetryService.ioObserver.observe(nativeThreadId);

            if (endDiskIODetails.isEmpty() == false) {
                IOMetrics diskIOMetrics = ThreadDiskIOMetricGenerator.calculateIOMetrics(
                    endDiskIODetails,
                    diskIOThreadStartDetails,
                    startTime,
                    endTime);

                if (diskIOMetrics != null) {
                    DiskTraceOperationMeters.readThroughputBps.record(diskIOMetrics.avgReadThroughputBps, attributes);
                    mapOfValues.put("readThroughputBps", diskIOMetrics.avgReadThroughputBps);
                    DiskTraceOperationMeters.writeThroughputBps.record(diskIOMetrics.avgWriteThroughputBps, attributes);
                    mapOfValues.put("writeThroughputBps", diskIOMetrics.avgWriteThroughputBps);
                    DiskTraceOperationMeters.totalThroughputBps.record(diskIOMetrics.avgTotalThroughputBps, attributes);
                    mapOfValues.put("totalThroughputBps", diskIOMetrics.avgTotalThroughputBps);
                    DiskTraceOperationMeters.readSyscallRate.record(diskIOMetrics.avgReadSyscallRate, attributes);
                    mapOfValues.put("readSyscallRate", diskIOMetrics.avgReadSyscallRate);
                    DiskTraceOperationMeters.writeSyscallRate.record(diskIOMetrics.avgWriteSyscallRate, attributes);
                    mapOfValues.put("writeSyscallRate", diskIOMetrics.avgWriteSyscallRate);
                    DiskTraceOperationMeters.totalSyscallRate.record(diskIOMetrics.avgTotalSyscallRate, attributes);
                    mapOfValues.put("totalSyscallRate", diskIOMetrics.avgTotalSyscallRate);
                    DiskTraceOperationMeters.pageCacheReadThroughputBps.record(diskIOMetrics.avgPageCacheReadThroughputBps, attributes);
                    mapOfValues.put("pageCacheReadThroughputBps", diskIOMetrics.avgPageCacheReadThroughputBps);
                    DiskTraceOperationMeters.pageCacheWriteThroughputBps.record(diskIOMetrics.avgPageCacheWriteThroughputBps, attributes);
                    mapOfValues.put("pageCacheWriteThroughputBps", diskIOMetrics.avgPageCacheWriteThroughputBps);
                    DiskTraceOperationMeters.pageCacheTotalThroughputBps.record(diskIOMetrics.avgPageCacheTotalThroughputBps, attributes);
                    mapOfValues.put("pageCacheTotalThroughputBps", diskIOMetrics.avgPageCacheTotalThroughputBps);
                }
            }
            Map<String, Object> endSchedODetails = OpenTelemetryService.schedObserver.observe(nativeThreadId);
            if (endSchedODetails.isEmpty() == false) {
                // TODO
                DiskTraceOperationMeters.schedRunTime.record(0, attributes);
                DiskTraceOperationMeters.schedWaitTime.record(0, attributes);
                DiskTraceOperationMeters.schedCtxRate.record(0, attributes);
            }
            if (threadState != null) {
                DiskTraceOperationMeters.heapAllocRate.record(threadState.heapAllocRate, attributes);
                mapOfValues.put("heapAllocRate", threadState.heapAllocRate);
                DiskTraceOperationMeters.blockedCount.add(threadState.blockedCount, attributes);
                mapOfValues.put("blockedCount", threadState.blockedCount);
                DiskTraceOperationMeters.blockedTime.add(threadState.blockedTime, attributes);
                mapOfValues.put("blockedTime", threadState.blockedTime);
                DiskTraceOperationMeters.waitedCount.add(threadState.waitedCount, attributes);
                mapOfValues.put("waitedCount", threadState.waitedCount);
                DiskTraceOperationMeters.waitedTime.add(threadState.waitedTime, attributes);
                mapOfValues.put("waitedTime", threadState.waitedTime);
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
