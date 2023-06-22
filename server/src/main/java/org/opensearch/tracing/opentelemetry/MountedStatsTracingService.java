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
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.opensearch.performanceanalyzer.commons.metrics.AllMetrics.DevicePartitionValue;

public class MountedStatsTracingService {
    private Map<PartitionKey, File> partitionKeyFileMap;
    private static final String MOUNT_POINT_KEY = "MountPoint";
    private static final String DEVICE_PARTITION_KEY = "DevicePartition";
    private static final MountedStatsTracingService INSTANCE;

    static {
        INSTANCE = new MountedStatsTracingService();
    }

    public static MountedStatsTracingService getInstance() {
        return INSTANCE;
    }

    private List<ObservableDoubleGauge> totalSpaceGauges;
    private List<ObservableDoubleGauge> freeSpaceGauges;
    private List<ObservableDoubleGauge> usableFreeSpaceGauges;

    private MountedStatsTracingService() {
        partitionKeyFileMap = new ConcurrentHashMap<>();
        totalSpaceGauges = new ArrayList<>();
        freeSpaceGauges = new ArrayList<>();
        usableFreeSpaceGauges = new ArrayList<>();
    }

    public void init(TracingServiceSettings tracingServiceSettings) {
        if (tracingServiceSettings.isTracingMountedPartitionMetricsEnabled()) {
            // TODO - Check with Rishabh if we are fine with loading the mount points on the start-up instead of having refresh
            loadMountedPartitions();
            Meter meter = OpenTelemetryService.periodicalMeter;
            initMetricGauges(meter);
        }

        tracingServiceSettings.addMountedPartitionMetricTracingSettingConsumer(this::toggleMountedStatsMetrics);
    }

    private void toggleMountedStatsMetrics(boolean mountedStatsMetricsEnabled) {
        if (mountedStatsMetricsEnabled) {
            stopGauges();
            loadMountedPartitions();
            Meter meter = OpenTelemetryService.periodicalMeter;
            initMetricGauges(meter);
        } else {
            stopGauges();
        }
    }

    private void stopGauges() {
        // Close the gauges and clear the list
        if (!totalSpaceGauges.isEmpty()) {
            totalSpaceGauges.forEach(it -> it.close());
            totalSpaceGauges.clear();
        }
        if (!freeSpaceGauges.isEmpty()) {
            freeSpaceGauges.forEach(it -> it.close());
            freeSpaceGauges.clear();
        }
        if (!usableFreeSpaceGauges.isEmpty()) {
            usableFreeSpaceGauges.forEach(it -> it.close());
            usableFreeSpaceGauges.clear();
        }
    }

    private void initMetricGauges(Meter meter) {
        // Get the disk stats per each mount point -
        for (Entry<PartitionKey, File> entry: partitionKeyFileMap.entrySet()) {
            Baggage baggage = Baggage.current();
            AttributesBuilder attributesBuilder = Attributes.builder();
            baggage.forEach((key, baggageEntry) -> attributesBuilder.put(key, baggageEntry.getValue()));
            attributesBuilder.put(MOUNT_POINT_KEY, entry.getKey().mountPoint);
            attributesBuilder.put(DEVICE_PARTITION_KEY, entry.getKey().devicePartition);
            Attributes attributes = attributesBuilder.build();

            var totalSpaceGauge = meter.gaugeBuilder(DevicePartitionValue.TOTAL_SPACE.name()).buildWithCallback(measurement ->
                measurement.record(entry.getValue().getTotalSpace(), attributes)
            );
            totalSpaceGauges.add(totalSpaceGauge);

            var freeSpaceGauge = meter.gaugeBuilder(DevicePartitionValue.FREE_SPACE.name()).buildWithCallback(measurement ->
                measurement.record(entry.getValue().getFreeSpace(), attributes)
            );
            freeSpaceGauges.add(freeSpaceGauge);

            var usableFreeSpaceGauge = meter.gaugeBuilder(DevicePartitionValue.USABLE_FREE_SPACE.name()).buildWithCallback(measurement ->
                measurement.record(entry.getValue().getUsableSpace(), attributes)
            );
            usableFreeSpaceGauges.add(usableFreeSpaceGauge);
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
