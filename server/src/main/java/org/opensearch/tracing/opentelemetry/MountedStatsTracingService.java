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
import io.opentelemetry.api.metrics.Meter;
import java.io.File;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.performanceanalyzer.commons.metrics.AllMetrics.DevicePartitionValue;

public class MountedStatsTracingService {
    private Map<PartitionKey, File> partitionKeyFileMap;
    private ScheduledExecutorService scheduler;
    private static final String MOUNT_POINT_KEY = "MountPoint";
    private static final String DEVICE_PARTITION_KEY = "DevicePartition";
    private static final int SCHEDULE_PERIOD = 5;
    private static final MountedStatsTracingService INSTANCE;

    static {
        INSTANCE = new MountedStatsTracingService();
    }

    public static MountedStatsTracingService getInstance() {
        return INSTANCE;
    }

    private MountedStatsTracingService() {
        partitionKeyFileMap = new ConcurrentHashMap<>();

        scheduler = new ScheduledThreadPoolExecutor(1, OpenSearchExecutors.daemonThreadFactory("OpenTelemetry-Mount-Scheduler"));
        // TODO - Check with Rishabh if we are fine with loading the mount points on the start-up instead of having refresh
        loadMountedPartitions();
        Meter meter = OpenTelemetryService.periodicalMeter;

        // Get the disk stats per each mount point -
        for (Entry<PartitionKey, File> entry: partitionKeyFileMap.entrySet()) {
            Baggage baggage = Baggage.current();
            AttributesBuilder attributesBuilder = Attributes.builder();
            baggage.forEach((key, baggageEntry) -> attributesBuilder.put(key, baggageEntry.getValue()));
            attributesBuilder.put(MOUNT_POINT_KEY, entry.getKey().mountPoint);
            attributesBuilder.put(DEVICE_PARTITION_KEY, entry.getKey().devicePartition);
            Attributes attributes = attributesBuilder.build();

            meter.gaugeBuilder(DevicePartitionValue.TOTAL_SPACE.name()).buildWithCallback(measurement ->
                measurement.record(entry.getValue().getTotalSpace(), attributes)
            );
            meter.gaugeBuilder(DevicePartitionValue.FREE_SPACE.name()).buildWithCallback(measurement ->
                measurement.record(entry.getValue().getFreeSpace(), attributes)
            );
            meter.gaugeBuilder(DevicePartitionValue.USABLE_FREE_SPACE.name()).buildWithCallback(measurement ->
                measurement.record(entry.getValue().getUsableSpace(), attributes)
            );
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

    private static class PartitionKey {
        private String mountPoint;
        private String devicePartition;

        public PartitionKey(String mountPoint, String devicePartition) {
            this.mountPoint = mountPoint;
            this.devicePartition = devicePartition;
        }
    }

    private void loadMountedPartitions() {
        Map<String, String> mountPointDevicePartitionMap = OpenTelemetryService.mountedPartitionObserver.observe();
        mountPointDevicePartitionMap.entrySet().forEach(entry -> partitionKeyFileMap.computeIfAbsent(new PartitionKey(entry.getKey(),
            entry.getValue()), partitionKey -> new File(partitionKey.mountPoint)));
    }
}
