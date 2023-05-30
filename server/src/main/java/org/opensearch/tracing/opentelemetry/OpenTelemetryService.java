/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry;

import com.sun.management.ThreadMXBean;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.BaggageBuilder;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.opensearch.action.ActionListener;
import org.opensearch.performanceanalyzer.commons.OSMetricsGeneratorFactory;
import org.opensearch.performanceanalyzer.commons.metrics.ThreadIDUtil;
import org.opensearch.performanceanalyzer.commons.metrics_generator.CPUPagingActivityGenerator;
import org.opensearch.performanceanalyzer.commons.metrics_generator.SchedMetricsGenerator;
import org.opensearch.performanceanalyzer.commons.metrics_generator.linux.LinuxDiskIOMetricsGenerator;
import org.opensearch.performanceanalyzer.commons.os.CPUObserver;
import org.opensearch.performanceanalyzer.commons.os.IOObserver;
import org.opensearch.performanceanalyzer.commons.os.SchedObserver;
import org.opensearch.performanceanalyzer.commons.os.ThreadDiskIO;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tracing.TaskEventListener;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.function.BiFunction;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;


public class OpenTelemetryService {
    public static Resource resource;
    public static SdkTracerProvider sdkTracerProvider;
    public static SdkMeterProvider sdkMeterProvider;
    public static Meter meter;
    public static OpenTelemetry openTelemetry;
    private static final List<String> allowedThreadPools = List.of(ThreadPool.Names.GENERIC);
    public static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    public static final CPUPagingActivityGenerator threadCPUPagingActivityGenerator =  OSMetricsGeneratorFactory.getInstance().getPagingActivityGenerator();

    public static final SchedMetricsGenerator schedMetricsGenerator = OSMetricsGeneratorFactory.getInstance().getSchedMetricsGenerator();

    public static final LinuxDiskIOMetricsGenerator diskIOMetricsGenerator = ThreadDiskIO.getIOUtilization();

    public static final CPUObserver cpuObserver = new CPUObserver();
    public static final IOObserver ioObserver = new IOObserver();
    public static final SchedObserver schedObserver = new SchedObserver();

    public static final ThreadIDUtil threadIdUtil = ThreadIDUtil.INSTANCE;

    static {
        // threadMXBean.setThreadContentionMonitoringEnabled(true);
        resource = Resource.getDefault()
            .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "opensearch-tasks-1")));

        OtlpGrpcSpanExporter exporter = OtlpGrpcSpanExporter.builder()
            .setEndpoint("http://localhost:4317") // Replace with the actual endpoint
            .build();

        OtlpGrpcMetricExporter metricExporter = OtlpGrpcMetricExporter.builder()
            .setEndpoint("http://localhost:4317") // Replace with the actual endpoint
            .build();
        sdkTracerProvider = SdkTracerProvider.builder()
            // .addSpanProcessor(SimpleSpanProcessor.create(OtlpHttpSpanExporter.builder().build()))
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .addSpanProcessor(BatchSpanProcessor.builder(LoggingSpanExporter.create()).build())
            .setResource(resource)
            .build();

        sdkMeterProvider = SdkMeterProvider.builder()
            .registerMetricReader(PeriodicMetricReader.builder(metricExporter).build())
            .setResource(resource)
            .build();

        openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setMeterProvider(sdkMeterProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal();

        meter = openTelemetry.meterBuilder("opensearch-task")
            .setInstrumentationVersion("1.0.0")
            .build();

    }

    public static boolean isThreadPoolAllowed(String threadPoolName) {
        return allowedThreadPools.contains(threadPoolName);
    }

    /**
     * starts the span and invokes the function under the scope of new span, closes the scope when function is invoked.
     * Wraps the ActionListener with {@link OTelContextPreservingActionListener} for context propagation and ends the span
     * on response/failure of action listener.
     */
    public static <R> void callFunctionAndStartSpan(String spanName, BiFunction<Object[], ActionListener<?>, R> function,
                                                    ActionListener<?> actionListener, Object... args) {
        callFunctionAndStartSpan(spanName, function, actionListener, Attributes.builder().build(), args);
    }

    public static <R> void callFunctionAndStartSpan(String spanName, BiFunction<Object[], ActionListener<?>, R> function,
                                                    ActionListener<?> actionListener, Attributes attributes, Object... args) {
        Context beforeAttach = Context.current();
        Span span = createSpan(spanName);
        Baggage baggage = createBaggage(spanName, attributes);
        Baggage beforeAttachBaggage = Baggage.current();
        try(Scope ignored = span.makeCurrent(); Scope ignored2 = baggage.makeCurrent()) {
            span.setAllAttributes(attributes);
            actionListener = new OTelContextPreservingActionListener<>(actionListener, beforeAttach, beforeAttachBaggage,
                span.getSpanContext().getSpanId());
            callTaskEventListeners(true, "", spanName + "-Start", Thread.currentThread(),
                TaskEventListeners.getInstance(null));
            function.apply(args, actionListener);
        } finally {
            callTaskEventListeners(false, "", spanName + "-End", Thread.currentThread(),
                TaskEventListeners.getInstance(null));
        }
    }
    /**
     * TODO - to be replaced when OpenSearch tracing APIs are available
     */
    private static Span createSpan(String spanName) {
        Tracer tracer = OpenTelemetryService.sdkTracerProvider.get("recover");
        Span span = tracer.spanBuilder(spanName).setParent(Context.current()).startSpan();
        span.setAttribute(stringKey("start-thread-name"), Thread.currentThread().getName());
        span.setAttribute(longKey("start-thread-id"), Thread.currentThread().getId());
        return span;
    }

    private static Baggage createBaggage(String spanName, Attributes attributes) {
        BaggageBuilder baggageBuilder = Baggage.builder();
        // only string keys and values are supported
        // copy all new + current attributes in new baggage
        attributes.forEach((k,v) -> baggageBuilder.put(k.getKey(), v.toString()));
        final int[] level = {0};
        Baggage.current().forEach((k,v) -> {
            baggageBuilder.put(k, v.getValue());
            if (k.startsWith("SpanName_")) {
                level[0] = Integer.parseInt(k.split("_")[1]) + 1;
            }
        });
        baggageBuilder.put("SpanName_"+ level[0], spanName);
        return baggageBuilder.build();
    }

    /**
     * Lazy initialization of all TaskEventListener.
     */
    public static class TaskEventListeners {
        static volatile List<TaskEventListener> INSTANCE;
        public static List<TaskEventListener> getInstance(List<TaskEventListener> otelEventListenerList) {
            if (INSTANCE == null) {
                synchronized (TaskEventListener.class) {
                    if (INSTANCE == null) {
                        INSTANCE = otelEventListenerList;
                        INSTANCE.add(JavaThreadEventListener.INSTANCE);
                        INSTANCE.add(DiskStatsEventListener.INSTANCE);
                    }
                }
            }
            return INSTANCE;
        }
    }

    protected static void callTaskEventListeners(boolean startEvent, String operationName, String eventName, Thread t,
                                               List<TaskEventListener> taskEventListeners) {
        if (Context.current() != Context.root()) {
            try (Scope ignored = Span.current().makeCurrent()) {
                if (taskEventListeners != null && !taskEventListeners.isEmpty()) {
                    for (TaskEventListener eventListener : taskEventListeners) {
                        if (eventListener.isApplicable(operationName, eventName)) {
                            if (startEvent) {
                                eventListener.onStart(operationName, eventName, t);
                            } else {
                                eventListener.onEnd(operationName, eventName, t);
                            }
                        }
                    }
                }
            }
        }
    }
}
