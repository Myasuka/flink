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
import org.apache.flink.runtime.state.internal.InternalListState;

import java.util.Collection;
import java.util.List;

/**
 * This class wraps list state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the user entry value of state
 */
class LatencyTrackingListState<K, N, T>
        extends AbstractLatencyTrackState<
                K,
                N,
                List<T>,
                InternalListState<K, N, T>,
                LatencyTrackingListState.ListStateLatencyMetrics>
        implements InternalListState<K, N, T> {

    LatencyTrackingListState(
            String stateName,
            InternalListState<K, N, T> original,
            LatencyTrackingStateConfig latencyTrackingStateConfig) {
        super(
                original,
                new ListStateLatencyMetrics(
                        stateName,
                        latencyTrackingStateConfig.getMetricGroup(),
                        latencyTrackingStateConfig.getSampleInterval(),
                        latencyTrackingStateConfig.getSlidingWindow()));
    }

    @Override
    public Iterable<T> get() throws Exception {
        return trackLatencyWithException(
                latencyTrackingStateMetric::checkGetCounter,
                () -> original.get(),
                latencyTrackingStateMetric::updateGetLatency);
    }

    @Override
    public void add(T value) throws Exception {
        trackLatencyWithException(
                latencyTrackingStateMetric::checkAddCounter,
                () -> original.add(value),
                latencyTrackingStateMetric::updateAddLatency);
    }

    @Override
    public List<T> getInternal() throws Exception {
        return original.getInternal();
    }

    @Override
    public void updateInternal(List<T> valueToStore) throws Exception {
        original.updateInternal(valueToStore);
    }

    @Override
    public void update(List<T> values) throws Exception {
        trackLatencyWithException(
                latencyTrackingStateMetric::checkUpdateCounter,
                () -> original.update(values),
                latencyTrackingStateMetric::updateUpdateLatency);
    }

    @Override
    public void addAll(List<T> values) throws Exception {
        trackLatencyWithException(
                latencyTrackingStateMetric::checkAddAllCounter,
                () -> original.addAll(values),
                latencyTrackingStateMetric::updateAddAllLatency);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        trackLatencyWithException(
                latencyTrackingStateMetric::checkMergeNamespacesCounter,
                () -> original.mergeNamespaces(target, sources),
                latencyTrackingStateMetric::updateMergeNamespacesLatency);
    }

    protected static class ListStateLatencyMetrics extends StateLatencyMetricBase {
        static final String LIST_STATE_GET_LATENCY = "listStateGetLatency";
        static final String LIST_STATE_ADD_LATENCY = "listStateAddLatency";
        static final String LIST_STATE_ADD_ALL_LATENCY = "listStateAddAllLatency";
        static final String LIST_STATE_UPDATE_LATENCY = "listStateUpdateLatency";
        static final String LIST_STATE_MERGE_NAMESPACES_LATENCY = "listStateMergeNamespacesLatency";

        ListStateLatencyMetrics(
                String stateName, MetricGroup metricGroup, int sampleInterval, long slidingWindow) {
            super(stateName, metricGroup, sampleInterval, slidingWindow);
        }

        boolean checkGetCounter() {
            return checkCounter(LIST_STATE_GET_LATENCY);
        }

        boolean checkAddCounter() {
            return checkCounter(LIST_STATE_ADD_LATENCY);
        }

        boolean checkAddAllCounter() {
            return checkCounter(LIST_STATE_ADD_ALL_LATENCY);
        }

        boolean checkUpdateCounter() {
            return checkCounter(LIST_STATE_UPDATE_LATENCY);
        }

        boolean checkMergeNamespacesCounter() {
            return checkCounter(LIST_STATE_MERGE_NAMESPACES_LATENCY);
        }

        void updateGetLatency(long duration) {
            updateHistogram(LIST_STATE_GET_LATENCY, duration);
        }

        void updateAddLatency(long duration) {
            updateHistogram(LIST_STATE_ADD_LATENCY, duration);
        }

        void updateAddAllLatency(long duration) {
            updateHistogram(LIST_STATE_ADD_ALL_LATENCY, duration);
        }

        void updateUpdateLatency(long duration) {
            updateHistogram(LIST_STATE_UPDATE_LATENCY, duration);
        }

        void updateMergeNamespacesLatency(long duration) {
            updateHistogram(LIST_STATE_MERGE_NAMESPACES_LATENCY, duration);
        }
    }
}
