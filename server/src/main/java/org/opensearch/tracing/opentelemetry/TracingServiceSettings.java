/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry;

import java.util.function.Consumer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Represents the tracing service settings
 */
public class TracingServiceSettings {

    private volatile boolean tracingNetworkMetricsEnabled;
    private volatile boolean tracingDiskMetricsEnabled;
    private volatile boolean tracingMountedPartitionMetricsEnabled;

    private volatile boolean tracingJavaThreadEnabled;

    private volatile boolean tracingDiskStatsMetricsEnabled;

    private ClusterSettings clusterSettings;
    public static final Setting<Boolean> NETWORK_METRICS_TRACING_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.metrics.network.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public void addNetworkMetricTracingSettingConsumer(Consumer<Boolean> consumer) {
        clusterSettings.addSettingsUpdateConsumer(NETWORK_METRICS_TRACING_ENABLED_SETTING, consumer);
    }

    public static final Setting<Boolean> DISK_METRICS_TRACING_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.metrics.disk.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public void addDiskMetricTracingSettingConsumer(Consumer<Boolean> consumer) {
        clusterSettings.addSettingsUpdateConsumer(DISK_METRICS_TRACING_ENABLED_SETTING, consumer);
    }
    public static final Setting<Boolean> MOUNTED_PARTITION_METRICS_TRACING_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.metrics.mounted_partitions.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public void addMountedPartitionMetricTracingSettingConsumer(Consumer<Boolean> consumer) {
        clusterSettings.addSettingsUpdateConsumer(MOUNTED_PARTITION_METRICS_TRACING_ENABLED_SETTING, consumer);
    }
    public static final Setting<Boolean> JAVA_THREAD_METRICS_TRACING_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.metrics.java_thread.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> DISK_STATS_METRICS_TRACING_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.metrics.disk_stats.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public TracingServiceSettings(Settings settings, ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;

        this.tracingNetworkMetricsEnabled = NETWORK_METRICS_TRACING_ENABLED_SETTING.get(settings);
        this.tracingDiskMetricsEnabled = DISK_METRICS_TRACING_ENABLED_SETTING.get(settings);
        this.tracingMountedPartitionMetricsEnabled = MOUNTED_PARTITION_METRICS_TRACING_ENABLED_SETTING.get(settings);
        this.tracingJavaThreadEnabled = JAVA_THREAD_METRICS_TRACING_ENABLED_SETTING.get(settings);
        this.tracingDiskStatsMetricsEnabled = DISK_STATS_METRICS_TRACING_ENABLED_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(NETWORK_METRICS_TRACING_ENABLED_SETTING, this::setTracingNetworkMetricsEnabled);
        clusterSettings.addSettingsUpdateConsumer(DISK_METRICS_TRACING_ENABLED_SETTING, this::setTracingDiskMetricsEnabled);
        clusterSettings.addSettingsUpdateConsumer(MOUNTED_PARTITION_METRICS_TRACING_ENABLED_SETTING, this::setTracingMountedPartitionMetricsEnabled);
        clusterSettings.addSettingsUpdateConsumer(JAVA_THREAD_METRICS_TRACING_ENABLED_SETTING, this::setTracingJavaThreadEnabled);
        clusterSettings.addSettingsUpdateConsumer(DISK_STATS_METRICS_TRACING_ENABLED_SETTING, this::setTracingDiskStatsMetricsEnabled);
    }

    public boolean isTracingNetworkMetricsEnabled() {
        return tracingNetworkMetricsEnabled;
    }

    public void setTracingNetworkMetricsEnabled(boolean tracingNetworkMetricsEnabled) {
        this.tracingNetworkMetricsEnabled = tracingNetworkMetricsEnabled;
    }

    public boolean isTracingDiskMetricsEnabled() {
        return tracingDiskMetricsEnabled;
    }

    public void setTracingDiskMetricsEnabled(boolean tracingDiskMetricsEnabled) {
        this.tracingDiskMetricsEnabled = tracingDiskMetricsEnabled;
    }

    public boolean isTracingMountedPartitionMetricsEnabled() {
        return tracingMountedPartitionMetricsEnabled;
    }

    public void setTracingMountedPartitionMetricsEnabled(boolean tracingMountedPartitionMetricsEnabled) {
        this.tracingMountedPartitionMetricsEnabled = tracingMountedPartitionMetricsEnabled;
    }

    public boolean isTracingJavaThreadEnabled() {
        return tracingJavaThreadEnabled;
    }

    public void setTracingJavaThreadEnabled(boolean tracingJavaThreadEnabled) {
        this.tracingJavaThreadEnabled = tracingJavaThreadEnabled;
    }

    public boolean isTracingDiskStatsMetricsEnabled() {
        return tracingDiskStatsMetricsEnabled;
    }

    public void setTracingDiskStatsMetricsEnabled(boolean tracingDiskStatsMetricsEnabled) {
        this.tracingDiskStatsMetricsEnabled = tracingDiskStatsMetricsEnabled;
    }
}
