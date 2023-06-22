/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.opensearch.test.OpenSearchTestCase;

public class MountedStatsTracingServiceTests extends OpenSearchTestCase {

    public void testInit() {
        MountedStatsTracingService mountedStatsTracingService = MountedStatsTracingService.getInstance();

        TracingServiceSettings tracingServiceSettings = mock(TracingServiceSettings.class);
        when(tracingServiceSettings.isTracingMountedPartitionMetricsEnabled()).thenReturn(false);
        doNothing().when(tracingServiceSettings).addNetworkMetricTracingSettingConsumer(any());

        mountedStatsTracingService.init(tracingServiceSettings);
        verify(tracingServiceSettings, times(1)).addMountedPartitionMetricTracingSettingConsumer(any());
    }
}
