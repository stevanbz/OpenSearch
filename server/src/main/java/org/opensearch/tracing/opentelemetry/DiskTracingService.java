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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.performanceanalyzer.commons.collectors.DiskMetrics;
import org.opensearch.performanceanalyzer.commons.hwnet.metrics.DiskMetricsCalculator;
import org.opensearch.performanceanalyzer.commons.hwnet.observer.impl.DiskObserver;
import org.opensearch.tracing.opentelemetry.meters.DiskStatsOperationMeters;

public class DiskTracingService {

    private static final DiskTracingService instance;
    private static final String DISK_METRICS_SCHEDULER = "OpenTelemetry-Disk-Scheduler";
    private ScheduledExecutorService scheduler;
    private long kvTimestamp = 0;
    private long oldkvTimestamp = 0;
    private Map<String, Map<String, Object>> metricsPerDisk;
    private Map<String, Map<String, Object>> oldMetricsPerDisk;
    private Scheduled scheduled;

    private Future diskMetricJob;
    private DiskObserver diskObserver;

    private static final int SCHEDULE_PERIOD = 5;

    static {
        instance = new DiskTracingService();
    }

    private DiskTracingService() {
        diskObserver = new DiskObserver();
        metricsPerDisk = new ConcurrentHashMap<>();
        oldMetricsPerDisk = new ConcurrentHashMap<>();
        scheduled = new Scheduled();
    }

    public void init(TracingServiceSettings tracingServiceSettings) {
        if (tracingServiceSettings.isTracingDiskMetricsEnabled()) {
            // Init scheduler
            scheduler = new ScheduledThreadPoolExecutor(1, OpenSearchExecutors.daemonThreadFactory(DISK_METRICS_SCHEDULER));
            diskMetricJob = scheduler.scheduleAtFixedRate(scheduled, SCHEDULE_PERIOD, SCHEDULE_PERIOD, TimeUnit.SECONDS);
        }

        tracingServiceSettings.addDiskMetricTracingSettingConsumer(this::toggleDiskStatsMetrics);
    }

    /**
     * Depending of the flag, enables or disables repeatable thread for getting the disk metrics
     * @param diskMetricsEnabled
     */
    private void toggleDiskStatsMetrics(boolean diskMetricsEnabled) {
        if (diskMetricsEnabled) {
            if (diskMetricJob != null || !diskMetricJob.isDone() || !diskMetricJob.isCancelled()) {
                diskMetricJob.cancel(true);
            }

            if (scheduler == null || scheduler.isShutdown() || scheduler.isTerminated()) {
                scheduler = new ScheduledThreadPoolExecutor(1, OpenSearchExecutors.daemonThreadFactory(
                    DISK_METRICS_SCHEDULER));
            }
            diskMetricJob = scheduler.scheduleAtFixedRate(scheduled, SCHEDULE_PERIOD, SCHEDULE_PERIOD, TimeUnit.SECONDS);
        } else {
            shutDown();
        }
    }
    public void shutDown() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(SCHEDULE_PERIOD, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static DiskTracingService getInstance() {
        return instance;
    }

    private final class Scheduled implements Runnable {
        private Scheduled() {
        }
        @Override
        public void run() {
            oldMetricsPerDisk.clear();
            oldMetricsPerDisk.putAll(metricsPerDisk);
            metricsPerDisk.clear();
            metricsPerDisk.putAll(diskObserver.observe());
            oldkvTimestamp = kvTimestamp;
            kvTimestamp = System.currentTimeMillis();

            Map<String, DiskMetrics> diskMetrics = DiskMetricsCalculator.calculateDiskMetrics(
                kvTimestamp,
                oldkvTimestamp,
                metricsPerDisk,
                oldMetricsPerDisk
            );

            if (diskMetrics != null) {
                for (Entry<String, DiskMetrics> entry: diskMetrics.entrySet()) {
                    Baggage baggage = Baggage.current();
                    AttributesBuilder attributesBuilder = Attributes.builder();

                    baggage.forEach((key, baggageEntry) -> {
                        attributesBuilder.put(key, baggageEntry.getValue());
                    });
                    Attributes attributes = attributesBuilder.build();
                    DiskStatsOperationMeters.utilization.record(entry.getValue().utilization, attributes);
                    DiskStatsOperationMeters.await.record(entry.getValue().await, attributes);
                    DiskStatsOperationMeters.serviceRate.record(entry.getValue().serviceRate, attributes);

                    DiskStatsOperationMeters.reset();
                }
            }
        }
    }
}
