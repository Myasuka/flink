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

import org.apache.flink.metrics.Histogram;
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
                        latencyTrackingStateConfig.getHistorySize()));
    }

    @Override
    public UV get(UK key) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnGet()) {
            return trackLatencyWithException(
                    () -> original.get(key),
                    (latency) -> latencyTrackingStateMetric.updateGetLatency(latency));
        } else {
            return original.get(key);
        }
    }

    @Override
    public void put(UK key, UV value) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnPut()) {
            trackLatencyWithException(
                    () -> original.put(key, value),
                    (latency) -> latencyTrackingStateMetric.updatePutLatency(latency));
        } else {
            original.put(key, value);
        }
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnPutAll()) {
            trackLatencyWithException(
                    () -> original.putAll(map),
                    (latency) -> latencyTrackingStateMetric.updatePutAllLatency(latency));
        } else {
            original.putAll(map);
        }
    }

    @Override
    public void remove(UK key) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnRemove()) {
            trackLatencyWithException(
                    () -> original.remove(key),
                    (latency) -> latencyTrackingStateMetric.updateRemoveLatency(latency));
        } else {
            original.remove(key);
        }
    }

    @Override
    public boolean contains(UK key) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnContains()) {
            return trackLatencyWithException(
                    () -> original.contains(key),
                    (latency) -> latencyTrackingStateMetric.updateContainsLatency(latency));
        } else {
            return original.contains(key);
        }
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnEntriesInit()) {
            return trackLatencyWithException(
                    () -> new IterableWrapper<>(original.entries()),
                    (latency) -> latencyTrackingStateMetric.updateEntriesInitLatency(latency));
        } else {
            return new IterableWrapper<>(original.entries());
        }
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnKeysInit()) {
            return trackLatencyWithException(
                    () -> new IterableWrapper<>(original.keys()),
                    (latency) -> latencyTrackingStateMetric.updateKeysInitLatency(latency));
        } else {
            return new IterableWrapper<>(original.keys());
        }
    }

    @Override
    public Iterable<UV> values() throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnValuesInit()) {
            return trackLatencyWithException(
                    () -> new IterableWrapper<>(original.values()),
                    (latency) -> latencyTrackingStateMetric.updateValuesInitLatency(latency));
        } else {
            return new IterableWrapper<>(original.values());
        }
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnIteratorInit()) {
            return trackLatencyWithException(
                    () -> new IteratorWrapper<>(original.iterator()),
                    (latency) -> latencyTrackingStateMetric.updateIteratorInitLatency(latency));
        } else {
            return new IteratorWrapper<>(original.iterator());
        }
    }

    @Override
    public boolean isEmpty() throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnIsEmpty()) {
            return trackLatencyWithException(
                    () -> original.isEmpty(),
                    (latency) -> latencyTrackingStateMetric.updateIsEmptyLatency(latency));
        } else {
            return original.isEmpty();
        }
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
            if (latencyTrackingStateMetric.trackLatencyOnIteratorHasNext()) {
                return trackLatency(
                        iterator::hasNext,
                        (latency) ->
                                latencyTrackingStateMetric.updateIteratorHasNextLatency(latency));
            } else {
                return iterator.hasNext();
            }
        }

        @Override
        public E next() {
            if (latencyTrackingStateMetric.trackLatencyOnIteratorNext()) {
                return trackLatency(
                        iterator::next,
                        (latency) -> latencyTrackingStateMetric.updateIteratorNextLatency(latency));
            } else {
                return iterator.next();
            }
        }

        @Override
        public void remove() {
            if (latencyTrackingStateMetric.trackLatencyOnIteratorRemove()) {
                trackLatency(
                        iterator::remove,
                        (latency) ->
                                latencyTrackingStateMetric.updateIteratorRemoveLatency(latency));
            } else {
                iterator.remove();
            }
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

        private int getCount = 0;
        private Histogram getOperationHistogram;
        private int iteratorRemoveCount = 0;
        private Histogram iteratorRemoveOperationHistogram;
        private int putCount = 0;
        private Histogram putOperationHistogram;
        private int putAllCount = 0;
        private Histogram putAllOperationHistogram;
        private int removeCount = 0;
        private Histogram removeOperationHistogram;
        private int containsCount = 0;
        private Histogram containsOperationHistogram;
        private int entriesInitCount = 0;
        private Histogram entriesInitOperationHistogram;
        private int keysInitCount = 0;
        private Histogram keysInitOperationHistogram;
        private int valuesInitCount = 0;
        private Histogram valuesInitOperationHistogram;
        private int isEmptyCount = 0;
        private Histogram isEmptyOperationHistogram;
        private int iteratorInitCount = 0;
        private Histogram iteratorInitOperationHistogram;
        private int iteratorHasNextCount = 0;
        private Histogram iteratorHasNextOperationHistogram;
        private int iteratorNextCount = 0;
        private Histogram iteratorNextOperationHistogram;

        MapStateLatencyMetrics(
                String stateName, MetricGroup metricGroup, int sampleInterval, int historySize) {
            super(stateName, metricGroup, sampleInterval, historySize);
        }

        protected void updateGetLatency(final long durationNanoTime) {
            if (getOperationHistogram == null) {
                getOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(MAP_STATE_GET_LATENCY, getOperationHistogram);
            }
            getOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnGet() {
            getCount = loopUpdateCounter(getCount);
            return getCount == 1;
        }

        void updatePutLatency(final long durationNanoTime) {
            if (putOperationHistogram == null) {
                putOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(MAP_STATE_PUT_LATENCY, putOperationHistogram);
            }
            putOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnPut() {
            putCount = loopUpdateCounter(putCount);
            return putCount == 1;
        }

        void updatePutAllLatency(final long durationNanoTime) {
            if (putAllOperationHistogram == null) {
                putAllOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(MAP_STATE_PUT_ALL_LATENCY, putAllOperationHistogram);
            }
            putAllOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnPutAll() {
            putAllCount = loopUpdateCounter(putAllCount);
            return putAllCount == 1;
        }

        void updateRemoveLatency(final long durationNanoTime) {
            if (removeOperationHistogram == null) {
                removeOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(MAP_STATE_REMOVE_LATENCY, removeOperationHistogram);
            }
            removeOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnRemove() {
            removeCount = loopUpdateCounter(removeCount);
            return removeCount == 1;
        }

        void updateContainsLatency(final long durationNanoTime) {
            if (containsOperationHistogram == null) {
                containsOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(MAP_STATE_CONTAINS_LATENCY, containsOperationHistogram);
            }
            containsOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnContains() {
            containsCount = loopUpdateCounter(containsCount);
            return containsCount == 1;
        }

        void updateEntriesInitLatency(final long durationNanoTime) {
            if (entriesInitOperationHistogram == null) {
                entriesInitOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(
                        MAP_STATE_ENTRIES_INIT_LATENCY, entriesInitOperationHistogram);
            }
            entriesInitOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnEntriesInit() {
            entriesInitCount = loopUpdateCounter(entriesInitCount);
            return entriesInitCount == 1;
        }

        void updateKeysInitLatency(final long durationNanoTime) {
            if (keysInitOperationHistogram == null) {
                keysInitOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(MAP_STATE_KEYS_INIT_LATENCY, keysInitOperationHistogram);
            }
            keysInitOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnKeysInit() {
            keysInitCount = loopUpdateCounter(keysInitCount);
            return keysInitCount == 1;
        }

        void updateValuesInitLatency(final long durationNanoTime) {
            if (valuesInitOperationHistogram == null) {
                valuesInitOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(MAP_STATE_VALUES_INIT_LATENCY, valuesInitOperationHistogram);
            }
            valuesInitOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnValuesInit() {
            valuesInitCount = loopUpdateCounter(valuesInitCount);
            return valuesInitCount == 1;
        }

        void updateIteratorInitLatency(final long durationNanoTime) {
            if (iteratorInitOperationHistogram == null) {
                iteratorInitOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(
                        MAP_STATE_ITERATOR_INIT_LATENCY, iteratorInitOperationHistogram);
            }
            iteratorInitOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnIteratorInit() {
            iteratorInitCount = loopUpdateCounter(iteratorInitCount);
            return iteratorInitCount == 1;
        }

        void updateIsEmptyLatency(final long durationNanoTime) {
            if (isEmptyOperationHistogram == null) {
                isEmptyOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(MAP_STATE_IS_EMPTY_LATENCY, isEmptyOperationHistogram);
            }
            isEmptyOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnIsEmpty() {
            isEmptyCount = loopUpdateCounter(isEmptyCount);
            return isEmptyCount == 1;
        }

        void updateIteratorHasNextLatency(final long durationNanoTime) {
            if (iteratorHasNextOperationHistogram == null) {
                iteratorHasNextOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(
                        MAP_STATE_ITERATOR_HAS_NEXT_LATENCY, iteratorHasNextOperationHistogram);
            }
            iteratorHasNextOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnIteratorHasNext() {
            iteratorHasNextCount = loopUpdateCounter(iteratorHasNextCount);
            return iteratorHasNextCount == 1;
        }

        void updateIteratorNextLatency(final long durationNanoTime) {
            if (iteratorNextOperationHistogram == null) {
                iteratorNextOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(
                        MAP_STATE_ITERATOR_NEXT_LATENCY, iteratorNextOperationHistogram);
            }
            iteratorNextOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnIteratorNext() {
            iteratorNextCount = loopUpdateCounter(iteratorNextCount);
            return iteratorNextCount == 1;
        }

        void updateIteratorRemoveLatency(final long durationNanoTime) {
            if (iteratorRemoveOperationHistogram == null) {
                iteratorRemoveOperationHistogram = histogramSupplier.get();
                metricGroup.histogram(
                        MAP_STATE_ITERATOR_REMOVE_LATENCY, iteratorRemoveOperationHistogram);
            }
            iteratorRemoveOperationHistogram.update(durationNanoTime);
        }

        boolean trackLatencyOnIteratorRemove() {
            iteratorRemoveCount = loopUpdateCounter(iteratorRemoveCount);
            return iteratorRemoveCount == 1;
        }
    }
}
