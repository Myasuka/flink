/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalValueState;

import java.io.IOException;

/**
 * This class wraps value state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the user value of state
 */
class LatencyTrackingValueState<K, N, T>
        extends AbstractLatencyTrackState<
                K,
                N,
                T,
                InternalValueState<K, N, T>,
                LatencyTrackingValueState.ValueStateLatencyMetrics>
        implements InternalValueState<K, N, T> {

    public LatencyTrackingValueState(
            String stateName,
            InternalValueState<K, N, T> original,
            LatencyTrackingStateConfig latencyTrackingStateConfig) {
        super(
                original,
                new ValueStateLatencyMetrics(
                        stateName,
                        latencyTrackingStateConfig.getMetricGroup(),
                        latencyTrackingStateConfig.getSampleInterval(),
                        latencyTrackingStateConfig.getHistorySize()));
    }

    @Override
    public T value() throws IOException {
        return trackLatencyWithIOException(
                latencyTrackingStateMetric::checkValueCounter,
                () -> original.value(),
                latencyTrackingStateMetric::updateGetLatency);
    }

    @Override
    public void update(T value) throws IOException {
        trackLatencyWithIOException(
                latencyTrackingStateMetric::checkUpdateCounter,
                () -> original.update(value),
                latencyTrackingStateMetric::updatePutLatency);
    }

    protected static class ValueStateLatencyMetrics extends StateLatencyMetricBase {
        static final String VALUE_STATE_GET_LATENCY = "valueStateGetLatency";
        static final String VALUE_STATE_UPDATE_LATENCY = "valueStateUpdateLatency";

        ValueStateLatencyMetrics(
                String stateName, MetricGroup metricGroup, int sampleInterval, int historySize) {
            super(stateName, metricGroup, sampleInterval, historySize);
        }

        boolean checkValueCounter() {
            return false;
        }

        boolean checkUpdateCounter() {
            return false;
        }

        void updateGetLatency(long duration) {
            updateHistogram(VALUE_STATE_GET_LATENCY, duration);
        }

        void updatePutLatency(long duration) {
            updateHistogram(VALUE_STATE_UPDATE_LATENCY, duration);
        }
    }
}
