/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;

import java.util.Collection;

/**
 * This class wraps aggregating state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <IN> Type of the value added to the state
 * @param <ACC> The type of the accumulator (intermediate aggregate state).
 * @param <OUT> Type of the value extracted from the state
 */
class LatencyTrackingAggregatingState<K, N, IN, ACC, OUT>
        extends AbstractLatencyTrackState<
                K,
                N,
                ACC,
                InternalAggregatingState<K, N, IN, ACC, OUT>,
                LatencyTrackingAggregatingState.AggregatingStateLatencyMetrics>
        implements InternalAggregatingState<K, N, IN, ACC, OUT> {

    LatencyTrackingAggregatingState(
            String stateName,
            InternalAggregatingState<K, N, IN, ACC, OUT> original,
            LatencyTrackingStateConfig latencyTrackingStateConfig) {
        super(
                original,
                new AggregatingStateLatencyMetrics(
                        stateName,
                        latencyTrackingStateConfig.getMetricGroup(),
                        latencyTrackingStateConfig.getSampleInterval(),
                        latencyTrackingStateConfig.getHistorySize()));
    }

    @Override
    public OUT get() throws Exception {
        return trackLatencyWithException(
                latencyTrackingStateMetric::checkGetCounter,
                () -> original.get(),
                latencyTrackingStateMetric::updateGetLatency);
    }

    @Override
    public void add(IN value) throws Exception {
        trackLatencyWithException(
                latencyTrackingStateMetric::checkAddCounter,
                () -> original.add(value),
                latencyTrackingStateMetric::updateAddLatency);
    }

    @Override
    public ACC getInternal() throws Exception {
        return original.getInternal();
    }

    @Override
    public void updateInternal(ACC valueToStore) throws Exception {
        original.updateInternal(valueToStore);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        trackLatencyWithException(
                latencyTrackingStateMetric::checkMergeNamespacesCounter,
                () -> original.mergeNamespaces(target, sources),
                latencyTrackingStateMetric::updateMergeNamespacesLatency);
    }

    protected static class AggregatingStateLatencyMetrics extends StateLatencyMetricBase {
        static final String AGGREGATING_STATE_GET_LATENCY = "aggregatingStateGetLatency";
        static final String AGGREGATING_STATE_ADD_LATENCY = "aggregatingStateAddLatency";
        static final String AGGREGATING_STATE_MERGE_NAMESPACES_LATENCY =
                "aggregatingStateMergeNamespacesLatency";

        AggregatingStateLatencyMetrics(
                String stateName, MetricGroup metricGroup, int sampleInterval, int historySize) {
            super(stateName, metricGroup, sampleInterval, historySize);
        }

        boolean checkGetCounter() {
            return checkCounter(AGGREGATING_STATE_GET_LATENCY);
        }

        boolean checkAddCounter() {
            return checkCounter(AGGREGATING_STATE_ADD_LATENCY);
        }

        boolean checkMergeNamespacesCounter() {
            return checkCounter(AGGREGATING_STATE_MERGE_NAMESPACES_LATENCY);
        }

        void updateGetLatency(long duration) {
            updateHistogram(AGGREGATING_STATE_GET_LATENCY, duration);
        }

        void updateAddLatency(long duration) {
            updateHistogram(AGGREGATING_STATE_ADD_LATENCY, duration);
        }

        void updateMergeNamespacesLatency(long duration) {
            updateHistogram(AGGREGATING_STATE_MERGE_NAMESPACES_LATENCY, duration);
        }
    }
}
