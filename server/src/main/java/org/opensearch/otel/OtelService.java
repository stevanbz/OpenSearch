/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.otel;

import com.sun.management.ThreadMXBean;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.OTelContextPreservingActionListener;

import java.lang.management.ManagementFactory;

import static io.opentelemetry.api.common.AttributeKey.doubleKey;
import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;


public class OtelService {
    public static String CPU_USAGE = "CPUUsage";
    public static String MEMORY_USAGE = "MemoryUsage";
    public static String CONTENTION_TIME = "ContentionTime";

    public static Resource resource;
    public static SdkTracerProvider sdkTracerProvider;
    public static SdkMeterProvider sdkMeterProvider;
    public static OpenTelemetry openTelemetry;
    private static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    static {
        resource = Resource.getDefault()
            .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "opensearch-tasks")));

        sdkTracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(OtlpHttpSpanExporter.builder().build()))
            .setResource(resource)
            .build();

        sdkMeterProvider = SdkMeterProvider.builder()
            .registerMetricReader(PeriodicMetricReader.builder(OtlpGrpcMetricExporter.builder().build()).build())
            .setResource(resource)
            .build();

        openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setMeterProvider(sdkMeterProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal();
    }

    public static long getCPUUsage(long threadId) {
        return threadMXBean.getThreadCpuTime(threadId);
    }

    public static long getMemoryUsage(long threadId) {
        return threadMXBean.getThreadAllocatedBytes(threadId);
    }

    public static long getThreadContentionTime(long threadId) {
        return threadMXBean.getThreadInfo(threadId).getBlockedTime();
    }
    public static <Response> ActionListener<Response> startSpan(String spanName, ActionListener<Response> actionListener) {
        Tracer tracer = OtelService.sdkTracerProvider.get("recover");
        Context beforeAttach = Context.current();
        Span span = tracer.spanBuilder(spanName).startSpan();
        span.setAttribute(stringKey("start-thread-name"), Thread.currentThread().getName());
        span.setAttribute(longKey("start-thread-id"), Thread.currentThread().getId());
        span.makeCurrent();
        return new OTelContextPreservingActionListener<>(beforeAttach, Context.current(), actionListener,
            span.getSpanContext().getSpanId());
    }

    /**
     * Adds event with cpu, memory and contention metrics
     * @param listener Listener which contains appropriate context
     * @param operationName Name of the operation logged in the event
     * @param <T>
     */
    public static <T> void addAfterSpanEvents(ActionListener<T> listener, String operationName) {
        if (listener instanceof OTelContextPreservingActionListener) {
            OTelContextPreservingActionListener wrappedListener = (OTelContextPreservingActionListener) listener;
            Context context = wrappedListener.getAfterAttachContext();
            try (Scope scope = context.makeCurrent()) {
                Span span = Span.fromContextOrNull(Context.current());
                if (span != Span.getInvalid()) {
                    Long currentThreadId = Thread.currentThread().getId();
                    span.addEvent("recover finished", Attributes.of(
                            stringKey("operation"), operationName,
                            doubleKey(CPU_USAGE), (OtelService.getCPUUsage(currentThreadId) - wrappedListener.getStartCPUUsage()) / 1000000.,
                            doubleKey(MEMORY_USAGE), (OtelService.getMemoryUsage(currentThreadId) - wrappedListener.getStartMemoryUsage()) / 1000000.,
                            doubleKey(CONTENTION_TIME), (OtelService.getThreadContentionTime(currentThreadId) - wrappedListener.getStartThreadContentionTime()) / 1000000.
                        )
                    );
                }
            }
        }
    }
}
