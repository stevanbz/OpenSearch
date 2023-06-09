/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry.meters;

import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import org.opensearch.performanceanalyzer.commons.collectors.NetInterfaceSummary.Direction;
import org.opensearch.performanceanalyzer.commons.metrics.AllMetrics.IPValue;
import org.opensearch.tracing.opentelemetry.OpenTelemetryService;

public class NetworkOperationMeters {

    public static DoubleHistogram inPacketRate4;
    public static DoubleHistogram inDropRate4;
    public static DoubleHistogram inPacketRate6;
    public static DoubleHistogram inDropRate6;
    public static DoubleHistogram inBps;

    public static DoubleHistogram outPacketRate4;
    public static DoubleHistogram outDropRate4;
    public static DoubleHistogram outPacketRate6;
    public static DoubleHistogram outDropRate6;
    public static DoubleHistogram outBps;

    static {
        Meter meter = getMeter();
        inPacketRate4 = meter.histogramBuilder(Direction.in.name() + "_" + IPValue.NET_PACKET_RATE4.name()).build();
        inDropRate4 = meter.histogramBuilder(Direction.in.name() + "_" + IPValue.NET_PACKET_DROP_RATE4.name()).build();
        inPacketRate6 = meter.histogramBuilder(Direction.in.name() + "_" + IPValue.NET_PACKET_RATE6.name()).build();
        inDropRate6 = meter.histogramBuilder(Direction.in.name() + "_" + IPValue.NET_PACKET_RATE6.name()).build();
        inBps = meter.histogramBuilder(Direction.in.name() + "_" + IPValue.NET_THROUGHPUT.name()).build();

        outPacketRate4 = meter.histogramBuilder(Direction.out.name() + "_" + IPValue.NET_PACKET_RATE4.name()).build();
        outDropRate4 = meter.histogramBuilder(Direction.out.name() + "_" + IPValue.NET_PACKET_DROP_RATE4.name()).build();
        outPacketRate6 = meter.histogramBuilder(Direction.out.name() + "_" + IPValue.NET_PACKET_RATE6.name()).build();
        outDropRate6 = meter.histogramBuilder(Direction.out.name() + "_" + IPValue.NET_PACKET_RATE6.name()).build();
        outBps = meter.histogramBuilder(Direction.out.name() + "_" + IPValue.NET_THROUGHPUT.name()).build();
    }

    public static void reset() {
        inPacketRate4.record(0);
        inDropRate4.record(0);
        inPacketRate6.record(0);
        inDropRate6.record(0);
        inBps.record(0);

        outPacketRate4.record(0);
        outDropRate4.record(0);
        outPacketRate6.record(0);
        outDropRate6.record(0);
        outBps.record(0);
    }

    public static Meter getMeter() {
        return OpenTelemetryService.periodicalMeter;
    }
}
