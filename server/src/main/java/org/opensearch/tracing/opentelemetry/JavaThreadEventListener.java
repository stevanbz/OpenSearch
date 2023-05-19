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
import org.opensearch.tracing.TaskEventListener;
import org.opensearch.tracing.opentelemetry.meters.TraceOperationMeters;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class JavaThreadEventListener implements TaskEventListener {
    public static JavaThreadEventListener INSTANCE = new JavaThreadEventListener();
    static class SupportedMeasurement {
        public long startTime;
        public long endTime;

        public double cpuTime;
        public double heapAllocatedBytes;
        public long blockedCount;
        public long waitedCount;
        public double blockedTime;
        public double waitedTime;

        public long duration;

        public Thread t;

        private SupportedMeasurement(Thread t) {
            startTime = System.currentTimeMillis();
            this.t = t;
            this.cpuTime = OpenTelemetryService.threadMXBean.getThreadCpuTime(t.getId())/1_000_000.;
            this.heapAllocatedBytes = OpenTelemetryService.threadMXBean.getThreadAllocatedBytes(t.getId());
            this.blockedCount = OpenTelemetryService.threadMXBean.getThreadInfo(t.getId()).getBlockedCount();
            this.waitedCount = OpenTelemetryService.threadMXBean.getThreadInfo(t.getId()).getWaitedCount();
            this.blockedTime = OpenTelemetryService.threadMXBean.getThreadInfo(t.getId()).getBlockedTime();
            this.waitedTime = OpenTelemetryService.threadMXBean.getThreadInfo(t.getId()).getWaitedTime();
        }

        public void endRecording() {
            endTime = System.currentTimeMillis();

            Baggage baggage = Baggage.current();

            AttributesBuilder attributesBuilder = Attributes.builder();

            baggage.forEach((key, baggageEntry) -> {
                attributesBuilder.put(key, baggageEntry.getValue());
            });

            Attributes attributes = attributesBuilder.build();

            TraceOperationMeters.cpuTime.record(OpenTelemetryService.threadMXBean.getThreadCpuTime(t.getId())/1_000_000. - this.cpuTime, attributes);
            TraceOperationMeters.heapAllocatedBytes.record(OpenTelemetryService.threadMXBean.getThreadAllocatedBytes(t.getId()) - this.heapAllocatedBytes,
                attributes);
            if (false) {
                TraceOperationMeters.blockedCount.add(OpenTelemetryService.threadMXBean.getThreadInfo(t.getId()).getBlockedCount() - this.blockedCount,
                    attributes
                );
                TraceOperationMeters.waitedCount.add(OpenTelemetryService.threadMXBean.getThreadInfo(t.getId()).getWaitedCount() - this.waitedCount, attributes
                );

                TraceOperationMeters.blockedTime.record(OpenTelemetryService.threadMXBean.getThreadInfo(t.getId()).getBlockedTime() - this.blockedTime, attributes
                );
                TraceOperationMeters.waitedTime.record(OpenTelemetryService.threadMXBean.getThreadInfo(t.getId()).getWaitedTime() - this.waitedTime, attributes
                );
            }
            this.duration = endTime - startTime;
            TraceOperationMeters.elapsedTime.record(duration, attributes);
            // meter.gaugeBuilder("CPUUtilization").buildWithCallback(callback -> cpuUtilization = callback);
            TraceOperationMeters.cpuUtilization.record(((OpenTelemetryService.threadMXBean.getThreadCpuTime(t.getId())/1_000_000. - this.cpuTime)/duration)*100, attributes);
        }
    }

    private static final Map<Long, Stack<SupportedMeasurement>> threadCPUUsage = new HashMap<>();
    @Override
    public void onStart(String operationName, String eventName, Thread t) {
        threadCPUUsage.computeIfAbsent(t.getId(), k -> new Stack<>());
        threadCPUUsage.get(t.getId()).add(new SupportedMeasurement(t));
    }

    @Override
    public void onEnd(String operationName, String eventName, Thread t) {
        SupportedMeasurement threadMeasurements = threadCPUUsage.get(t.getId()).peek();
        threadMeasurements.endRecording();
        threadCPUUsage.get(t.getId()).pop();
    }

    @Override
    public boolean isApplicable(String operationName, String eventName) {
        return true;
    }
}
