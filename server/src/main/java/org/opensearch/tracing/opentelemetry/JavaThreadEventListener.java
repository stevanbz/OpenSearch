/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import org.opensearch.tracing.TaskEventListener;
import org.opensearch.tracing.opentelemetry.meters.TraceOperationMeters;

import java.util.HashMap;
import java.util.LinkedList;
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
            TraceOperationMeters.cpuTime.record(OpenTelemetryService.threadMXBean.getThreadCpuTime(t.getId())/1_000_000. - this.cpuTime, Attributes.of(                        AttributeKey.longKey("ThreadID"), t.getId(),
                AttributeKey.stringKey("ThreadName"), t.getName(),
                AttributeKey.stringKey("SpanID"), Span.current().getSpanContext().getSpanId())
            );
            TraceOperationMeters.heapAllocatedBytes.record(OpenTelemetryService.threadMXBean.getThreadAllocatedBytes(t.getId()) - this.heapAllocatedBytes, Attributes.of(                        AttributeKey.longKey("ThreadID"), t.getId(),
                AttributeKey.stringKey("ThreadName"), t.getName(),
                AttributeKey.stringKey("SpanID"), Span.current().getSpanContext().getSpanId())
            );
            if (false) {
                TraceOperationMeters.blockedCount.add(OpenTelemetryService.threadMXBean.getThreadInfo(t.getId()).getBlockedCount() - this.blockedCount, Attributes.of(                        AttributeKey.longKey("ThreadID"), t.getId(),
                    AttributeKey.stringKey("ThreadName"), t.getName(),
                    AttributeKey.stringKey("SpanID"), Span.current().getSpanContext().getSpanId())
                );
                TraceOperationMeters.waitedCount.add(OpenTelemetryService.threadMXBean.getThreadInfo(t.getId()).getWaitedCount() - this.waitedCount, Attributes.of(                        AttributeKey.longKey("ThreadID"), t.getId(),
                    AttributeKey.stringKey("ThreadName"), t.getName(),
                    AttributeKey.stringKey("SpanID"), Span.current().getSpanContext().getSpanId())
                );

                TraceOperationMeters.blockedTime.record(OpenTelemetryService.threadMXBean.getThreadInfo(t.getId()).getBlockedTime() - this.blockedTime, Attributes.of(AttributeKey.longKey("ThreadID"), t.getId(),
                    AttributeKey.stringKey("ThreadName"), t.getName(),
                    AttributeKey.stringKey("SpanID"), Span.current().getSpanContext().getSpanId())
                );
                TraceOperationMeters.waitedTime.record(OpenTelemetryService.threadMXBean.getThreadInfo(t.getId()).getWaitedTime() - this.waitedTime, Attributes.of(AttributeKey.longKey("ThreadID"), t.getId(),
                    AttributeKey.stringKey("ThreadName"), t.getName(),
                    AttributeKey.stringKey("SpanID"), Span.current().getSpanContext().getSpanId())
                );
            }
            TraceOperationMeters.elapsedTime.record(startTime - endTime, Attributes.of(AttributeKey.longKey("ThreadID"), t.getId(),
                AttributeKey.stringKey("ThreadName"), t.getName(),
                AttributeKey.stringKey("SpanID"), Span.current().getSpanContext().getSpanId())
            );
            TraceOperationMeters.cpuUtilization.record(((OpenTelemetryService.threadMXBean.getThreadCpuTime(t.getId())/1_000_000. - this.cpuTime)/duration)*100, Attributes.of(                        AttributeKey.longKey("ThreadID"), t.getId(),
                AttributeKey.stringKey("ThreadName"), t.getName(),
                AttributeKey.stringKey("SpanID"), Span.current().getSpanContext().getSpanId())
            );
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
