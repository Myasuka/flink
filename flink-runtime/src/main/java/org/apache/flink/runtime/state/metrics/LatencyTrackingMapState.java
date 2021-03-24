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
import org.apache.flink.runtime.state.internal.InternalMapState;

import java.util.Iterator;
import java.util.Map;

/**
 * This class wraps map state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <UK> Type of the user entry key of state
 * @param <UV> Type of the user entry value of state
 */
class LatencyTrackingMapState<K, N, UK, UV>
        extends AbstractLatencyTrackState<
                K,
                N,
                Map<UK, UV>,
                InternalMapState<K, N, UK, UV>,
                LatencyTrackingMapState.MapStateLatencyMetrics>
        implements InternalMapState<K, N, UK, UV> {
    LatencyTrackingMapState(
            String stateName,
            InternalMapState<K, N, UK, UV> original,
            LatencyTrackingStateConfig latencyTrackingStateConfig) {
        super(
                original,
                new MapStateLatencyMetrics(
                        stateName,
                        latencyTrackingStateConfig.getMetricGroup(),
                        latencyTrackingStateConfig.getSampleInterval(),
                        latencyTrackingStateConfig.getSlidingWindow()));
    }

    @Override
    public UV get(UK key) throws Exception {
        return trackLatencyWithException(
                latencyTrackingStateMetric::checkGetCounter,
                () -> original.get(key),
                latencyTrackingStateMetric::updateGetLatency);
    }

    @Override
    public void put(UK key, UV value) throws Exception {
        trackLatencyWithException(
                latencyTrackingStateMetric::checkPutCounter,
                () -> original.put(key, value),
                latencyTrackingStateMetric::updatePutLatency);
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        trackLatencyWithException(
                latencyTrackingStateMetric::checkPuAllCounter,
                () -> original.putAll(map),
                latencyTrackingStateMetric::updatePutAllLatency);
    }

    @Override
    public void remove(UK key) throws Exception {
        trackLatencyWithException(
                latencyTrackingStateMetric::checkRemoveCounter,
                () -> original.remove(key),
                latencyTrackingStateMetric::updateRemoveLatency);
    }

    @Override
    public boolean contains(UK key) throws Exception {
        return trackLatencyWithException(
                latencyTrackingStateMetric::checkContainsCounter,
                () -> original.contains(key),
                latencyTrackingStateMetric::updateContainsLatency);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        return trackLatencyWithException(
                latencyTrackingStateMetric::checkEntriesCounter,
                () -> new IterableWrapper<>(original.entries()),
                latencyTrackingStateMetric::updateEntriesLatency);
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        return trackLatencyWithException(
                latencyTrackingStateMetric::checkKeysCounter,
                () -> new IterableWrapper<>(original.keys()),
                latencyTrackingStateMetric::updateKeysLatency);
    }

    @Override
    public Iterable<UV> values() throws Exception {
        return trackLatencyWithException(
                latencyTrackingStateMetric::checkValuesCounter,
                () -> new IterableWrapper<>(original.values()),
                latencyTrackingStateMetric::updateValuesLatency);
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        return trackLatencyWithException(
                latencyTrackingStateMetric::checkIteratorCounter,
                () -> new IteratorWrapper<>(original.iterator()),
                latencyTrackingStateMetric::updateIteratorLatency);
    }

    @Override
    public boolean isEmpty() throws Exception {
        return trackLatencyWithException(
                latencyTrackingStateMetric::checkIsEmptyCounter,
                () -> original.isEmpty(),
                latencyTrackingStateMetric::updateIsEmptyLatency);
    }

    private class IterableWrapper<E> implements Iterable<E> {
        private final Iterable<E> iterable;

        IterableWrapper(Iterable<E> iterable) {
            this.iterable = iterable;
        }

        @Override
        public Iterator<E> iterator() {
            return new IteratorWrapper<>(iterable.iterator());
        }
    }

    private class IteratorWrapper<E> implements Iterator<E> {
        private final Iterator<E> iterator;

        IteratorWrapper(Iterator<E> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return trackLatency(
                    latencyTrackingStateMetric::checkIteratorHasNextCounter,
                    iterator::hasNext,
                    latencyTrackingStateMetric::updateIteratorHasNextLatency);
        }

        @Override
        public E next() {
            return trackLatency(
                    latencyTrackingStateMetric::checkIteratorNextCounter,
                    iterator::next,
                    latencyTrackingStateMetric::updateIteratorNextLatency);
        }

        @Override
        public void remove() {
            trackLatency(
                    latencyTrackingStateMetric::checkIteratorRemoveCounter,
                    iterator::remove,
                    latencyTrackingStateMetric::updateIteratorRemoveLatency);
        }
    }

    protected static class MapStateLatencyMetrics extends StateLatencyMetricBase {
        static final String MAP_STATE_GET_LATENCY = "mapStateGetLatency";
        static final String MAP_STATE_PUT_LATENCY = "mapStatePutLatency";
        static final String MAP_STATE_PUT_ALL_LATENCY = "mapStatePutAllLatency";
        static final String MAP_STATE_REMOVE_LATENCY = "mapStateRemoveLatency";
        static final String MAP_STATE_CONTAINS_LATENCY = "mapStateContainsAllLatency";
        static final String MAP_STATE_ENTRIES_INIT_LATENCY = "mapStateEntriesInitLatency";
        static final String MAP_STATE_KEYS_INIT_LATENCY = "mapStateKeysInitLatency";
        static final String MAP_STATE_VALUES_INIT_LATENCY = "mapStateValuesInitLatency";
        static final String MAP_STATE_ITERATOR_INIT_LATENCY = "mapStateIteratorInitLatency";
        static final String MAP_STATE_IS_EMPTY_LATENCY = "mapStateIsEmptyLatency";
        static final String MAP_STATE_ITERATOR_HAS_NEXT_LATENCY = "mapStateIteratorHasNextLatency";
        static final String MAP_STATE_ITERATOR_NEXT_LATENCY = "mapStateIteratorNextLatency";
        static final String MAP_STATE_ITERATOR_REMOVE_LATENCY = "mapStateIteratorRemoveLatency";

        MapStateLatencyMetrics(
                String stateName, MetricGroup metricGroup, int sampleInterval, long slidingWindow) {
            super(stateName, metricGroup, sampleInterval, slidingWindow);
        }

        boolean checkGetCounter() {
            return checkCounter(MAP_STATE_GET_LATENCY);
        }

        boolean checkPutCounter() {
            return checkCounter(MAP_STATE_PUT_LATENCY);
        }

        boolean checkPuAllCounter() {
            return checkCounter(MAP_STATE_PUT_ALL_LATENCY);
        }

        boolean checkRemoveCounter() {
            return checkCounter(MAP_STATE_REMOVE_LATENCY);
        }

        boolean checkContainsCounter() {
            return checkCounter(MAP_STATE_CONTAINS_LATENCY);
        }

        boolean checkEntriesCounter() {
            return checkCounter(MAP_STATE_ENTRIES_INIT_LATENCY);
        }

        boolean checkKeysCounter() {
            return checkCounter(MAP_STATE_KEYS_INIT_LATENCY);
        }

        boolean checkValuesCounter() {
            return checkCounter(MAP_STATE_VALUES_INIT_LATENCY);
        }

        boolean checkIteratorCounter() {
            return checkCounter(MAP_STATE_ITERATOR_INIT_LATENCY);
        }

        boolean checkIsEmptyCounter() {
            return checkCounter(MAP_STATE_IS_EMPTY_LATENCY);
        }

        boolean checkIteratorHasNextCounter() {
            return checkCounter(MAP_STATE_ITERATOR_HAS_NEXT_LATENCY);
        }

        boolean checkIteratorNextCounter() {
            return checkCounter(MAP_STATE_ITERATOR_NEXT_LATENCY);
        }

        boolean checkIteratorRemoveCounter() {
            return checkCounter(MAP_STATE_ITERATOR_REMOVE_LATENCY);
        }

        void updateGetLatency(long duration) {
            updateHistogram(MAP_STATE_GET_LATENCY, duration);
        }

        void updatePutLatency(long duration) {
            updateHistogram(MAP_STATE_PUT_LATENCY, duration);
        }

        void updatePutAllLatency(long duration) {
            updateHistogram(MAP_STATE_PUT_ALL_LATENCY, duration);
        }

        void updateRemoveLatency(long duration) {
            updateHistogram(MAP_STATE_REMOVE_LATENCY, duration);
        }

        void updateContainsLatency(long duration) {
            updateHistogram(MAP_STATE_CONTAINS_LATENCY, duration);
        }

        void updateEntriesLatency(long duration) {
            updateHistogram(MAP_STATE_ENTRIES_INIT_LATENCY, duration);
        }

        void updateKeysLatency(long duration) {
            updateHistogram(MAP_STATE_KEYS_INIT_LATENCY, duration);
        }

        void updateValuesLatency(long duration) {
            updateHistogram(MAP_STATE_VALUES_INIT_LATENCY, duration);
        }

        void updateIteratorLatency(long duration) {
            updateHistogram(MAP_STATE_ITERATOR_INIT_LATENCY, duration);
        }

        void updateIsEmptyLatency(long duration) {
            updateHistogram(MAP_STATE_IS_EMPTY_LATENCY, duration);
        }

        void updateIteratorHasNextLatency(long duration) {
            updateHistogram(MAP_STATE_ITERATOR_HAS_NEXT_LATENCY, duration);
        }

        void updateIteratorNextLatency(long duration) {
            updateHistogram(MAP_STATE_ITERATOR_NEXT_LATENCY, duration);
        }

        void updateIteratorRemoveLatency(long duration) {
            updateHistogram(MAP_STATE_ITERATOR_REMOVE_LATENCY, duration);
        }
    }
}
