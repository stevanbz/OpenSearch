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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.performanceanalyzer.commons.collectors.NetInterfaceSummary;
import org.opensearch.performanceanalyzer.commons.hwnet.metrics.NetworkMetricsCalculator;
import org.opensearch.performanceanalyzer.commons.hwnet.observer.impl.DeviceNetworkStatsObserver;
import org.opensearch.performanceanalyzer.commons.hwnet.observer.impl.Ipv4Observer;
import org.opensearch.performanceanalyzer.commons.hwnet.observer.impl.Ipv6Observer;
import org.opensearch.tracing.opentelemetry.meters.NetworkOperationMeters;

public class NetworkTracingService {

    private static final NetworkTracingService instance;

    private ScheduledExecutorService scheduler;
    private long kvTimestamp = 0;
    private long oldkvTimestamp = 0;

    private Map<String, Long> oldIpv4Metrics;
    private Map<String, Long> oldIpv6Metrics;
    private Map<String, Long> oldDeviceNetworkStatsMetrics;

    private Map<String, Long> ipv4Metrics;
    private Map<String, Long> ipv6Metrics;
    private Map<String, Long> deviceNetworkStatsMetrics;

    private Ipv4Observer ipv4Observer;
    private Ipv6Observer ipv6Observer;
    private DeviceNetworkStatsObserver deviceNetworkStatsObserver;

    private Scheduled scheduled;

    private static final int SCHEDULE_PERIOD = 5;

    static {
        instance = new NetworkTracingService();
    }

    private NetworkTracingService() {
        ipv4Observer = new Ipv4Observer();
        ipv6Observer = new Ipv6Observer();
        deviceNetworkStatsObserver = new DeviceNetworkStatsObserver();

        oldIpv4Metrics = new ConcurrentHashMap<>();
        oldIpv6Metrics = new ConcurrentHashMap<>();
        oldDeviceNetworkStatsMetrics = new ConcurrentHashMap<>();

        ipv4Metrics = new ConcurrentHashMap<>();
        ipv6Metrics = new ConcurrentHashMap<>();
        deviceNetworkStatsMetrics = new ConcurrentHashMap<>();

        scheduled = new Scheduled();
        scheduler = new ScheduledThreadPoolExecutor(1, OpenSearchExecutors.daemonThreadFactory("OpenTelemetry-Network-Scheduler"));
        scheduler.scheduleAtFixedRate(scheduled, SCHEDULE_PERIOD, SCHEDULE_PERIOD, TimeUnit.SECONDS);
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

    public static NetworkTracingService getInstance() {
        return instance;
    }

    private final class Scheduled implements Runnable {
        private Scheduled() {
        }

        @Override
        public void run() {
            Baggage baggage = Baggage.current();
            AttributesBuilder attributesBuilder = Attributes.builder();
            baggage.forEach((key, baggageEntry) -> {
                attributesBuilder.put(key, baggageEntry.getValue());
            });

            Attributes attributes = attributesBuilder.build();

            oldIpv4Metrics.clear();
            oldIpv4Metrics.putAll(ipv4Metrics);
            ipv4Metrics.clear();
            ipv4Metrics.putAll(ipv4Observer.observe());

            oldIpv6Metrics.clear();
            oldIpv6Metrics.putAll(ipv6Metrics);
            ipv6Metrics.clear();
            ipv6Metrics.putAll(ipv6Observer.observe());

            oldDeviceNetworkStatsMetrics.clear();
            oldDeviceNetworkStatsMetrics.putAll(deviceNetworkStatsMetrics);
            deviceNetworkStatsMetrics.clear();
            deviceNetworkStatsMetrics.putAll(deviceNetworkStatsObserver.observe());

            oldkvTimestamp = kvTimestamp;
            kvTimestamp = System.currentTimeMillis();

            if (!oldIpv6Metrics.isEmpty() && !oldIpv4Metrics.isEmpty() && !oldDeviceNetworkStatsMetrics.isEmpty()) {
                NetInterfaceSummary inNetworkMetrics = NetworkMetricsCalculator.calculateInNetworkMetrics(
                    kvTimestamp,
                    oldkvTimestamp,
                    ipv4Metrics,
                    oldIpv4Metrics,
                    ipv6Metrics,
                    oldIpv6Metrics,
                    deviceNetworkStatsMetrics,
                    oldDeviceNetworkStatsMetrics
                );

                NetInterfaceSummary outNetworkMetrics = NetworkMetricsCalculator.calculateOutNetworkMetrics(
                    kvTimestamp,
                    oldkvTimestamp,
                    ipv4Metrics,
                    oldIpv4Metrics,
                    ipv6Metrics,
                    oldIpv6Metrics,
                    deviceNetworkStatsMetrics,
                    oldDeviceNetworkStatsMetrics
                );
                if (inNetworkMetrics != null) {
                    NetworkOperationMeters.inPacketRate4.record(inNetworkMetrics.getPacketRate4(), attributes);
                    NetworkOperationMeters.inDropRate4.record(inNetworkMetrics.getDropRate4(), attributes);
                    NetworkOperationMeters.inPacketRate6.record(inNetworkMetrics.getPacketRate6(), attributes);
                    NetworkOperationMeters.inDropRate6.record(inNetworkMetrics.getPacketRate6(), attributes);
                    NetworkOperationMeters.inBps.record(inNetworkMetrics.getBps(), attributes);
                }

                if (outNetworkMetrics != null) {
                    NetworkOperationMeters.outPacketRate4.record(outNetworkMetrics.getPacketRate4(), attributes);
                    NetworkOperationMeters.outDropRate4.record(outNetworkMetrics.getDropRate4(), attributes);
                    NetworkOperationMeters.outPacketRate6.record(outNetworkMetrics.getPacketRate6(), attributes);
                    NetworkOperationMeters.outDropRate6.record(outNetworkMetrics.getPacketRate6(), attributes);
                    NetworkOperationMeters.outBps.record(outNetworkMetrics.getBps(), attributes);
                }
                NetworkOperationMeters.reset();
            }
        }
    }
}
