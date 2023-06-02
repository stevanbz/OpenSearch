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
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import org.opensearch.performanceanalyzer.commons.collectors.NetInterfaceSummary;
import org.opensearch.performanceanalyzer.commons.os.metrics.NetworkMetricsCalculator;
import org.opensearch.tracing.TaskEventListener;
import org.opensearch.tracing.opentelemetry.meters.NetworkOperationMeters;

public class NetworkStatsEventListener implements TaskEventListener {
    public static NetworkStatsEventListener INSTANCE = new NetworkStatsEventListener();

    private static final Map<Long, Stack<SupportedMeasurement>> networkStats = new HashMap<>();

    static class SupportedMeasurement {
        public long startTime;
        public long endTime;
        private Map<String, Long> ipv4Metrics;
        private Map<String, Long> ipv6Metrics;
        private Map<String, Long> deviceNetworkStatsMetrics;

        private SupportedMeasurement() {
            this.startTime = System.currentTimeMillis();
            this.ipv4Metrics = OpenTelemetryService.ipv4Observer.observe();
            this.ipv6Metrics = OpenTelemetryService.ipv6Observer.observe();
            this.deviceNetworkStatsMetrics = OpenTelemetryService.deviceNetworkStatsObserver.observe();
        }

        public void endRecording() {
            this.endTime = System.currentTimeMillis();
            Baggage baggage = Baggage.current();
            AttributesBuilder attributesBuilder = Attributes.builder();
            baggage.forEach((key, baggageEntry) -> {
                attributesBuilder.put(key, baggageEntry.getValue());
            });

            Attributes attributes = attributesBuilder.build();

            Map<String, Long> endIpv4Metrics = OpenTelemetryService.ipv4Observer.observe();
            Map<String, Long> endIpv6Metrics = OpenTelemetryService.ipv6Observer.observe();
            Map<String, Long> endDeviceNetworkStatsMetrics = OpenTelemetryService.deviceNetworkStatsObserver.observe();
            Map<String, Object> mapOfValues = new HashMap<>();
            if (endIpv4Metrics != null && endIpv6Metrics != null && endDeviceNetworkStatsMetrics != null) {
                NetInterfaceSummary inNetworkMetrics = NetworkMetricsCalculator.calculateInNetworkMetrics(
                    endTime,
                    startTime,
                    endIpv4Metrics,
                    ipv4Metrics,
                    endIpv6Metrics,
                    ipv6Metrics,
                    endDeviceNetworkStatsMetrics,
                    deviceNetworkStatsMetrics
                );

                NetInterfaceSummary outNetworkMetrics = NetworkMetricsCalculator.calculateOutNetworkMetrics(
                    endTime,
                    startTime,
                    endIpv4Metrics,
                    ipv4Metrics,
                    endIpv6Metrics,
                    ipv6Metrics,
                    endDeviceNetworkStatsMetrics,
                    deviceNetworkStatsMetrics
                );

                if (inNetworkMetrics != null) {
                    NetworkOperationMeters.inPacketRate4.record(inNetworkMetrics.getPacketRate4(), attributes);
                    mapOfValues.put("inPacketRate4", inNetworkMetrics.getPacketRate4());

                    NetworkOperationMeters.inDropRate4.record(inNetworkMetrics.getDropRate4(), attributes);
                    mapOfValues.put("inDropRate4", inNetworkMetrics.getDropRate4());

                    NetworkOperationMeters.inPacketRate6.record(inNetworkMetrics.getPacketRate6(), attributes);
                    mapOfValues.put("inPacketRate6", inNetworkMetrics.getPacketRate6());

                    NetworkOperationMeters.inDropRate6.record(inNetworkMetrics.getPacketRate6(), attributes);
                    mapOfValues.put("inDropRate6", inNetworkMetrics.getPacketRate6());

                    NetworkOperationMeters.inBps.record(inNetworkMetrics.getBps(), attributes);
                    mapOfValues.put("inBps", inNetworkMetrics.getBps());
                }

                if (outNetworkMetrics != null) {
                    NetworkOperationMeters.outPacketRate4.record(outNetworkMetrics.getPacketRate4(), attributes);
                    mapOfValues.put("outPacketRate4", outNetworkMetrics.getPacketRate4());

                    NetworkOperationMeters.outDropRate4.record(outNetworkMetrics.getDropRate4(), attributes);
                    mapOfValues.put("outDropRate4", outNetworkMetrics.getDropRate4());

                    NetworkOperationMeters.outPacketRate6.record(outNetworkMetrics.getPacketRate6(), attributes);
                    mapOfValues.put("outPacketRate6", outNetworkMetrics.getPacketRate6());

                    NetworkOperationMeters.outDropRate6.record(outNetworkMetrics.getPacketRate6(), attributes);
                    mapOfValues.put("outDropRate6", outNetworkMetrics.getPacketRate6());

                    NetworkOperationMeters.outBps.record(outNetworkMetrics.getBps(), attributes);
                    mapOfValues.put("outBps", outNetworkMetrics.getBps());
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
                System.out.println("\nNetwork stats Done");
                resetHistogram();
            }
        }
    }

    private static void resetHistogram() {
        NetworkOperationMeters.reset();
    }

    @Override
    public void onStart(String operationName, String eventName, Thread t) {
        networkStats.computeIfAbsent(t.getId(), k -> new Stack<>());
        networkStats.get(t.getId()).add(new SupportedMeasurement());
    }

    @Override
    public void onEnd(String operationName, String eventName, Thread t) {
        SupportedMeasurement networkMeasurement = networkStats.get(t.getId()).peek();
        networkMeasurement.endRecording();
        networkStats.get(t.getId()).pop();
    }

    @Override
    public boolean isApplicable(String operationName, String eventName) {
        return true;
    }
}
