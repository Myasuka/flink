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

public class LatencyTrackingMapState<K, N, UK, UV>
        extends AbstractLatencyTrackState<
                K,
                N,
                Map<UK, UV>,
                InternalMapState<K, N, UK, UV>,
                LatencyTrackingMapState.LatencyTrackingMapStateMetrics>
        implements InternalMapState<K, N, UK, UV> {
    LatencyTrackingMapState(
            String stateName,
            InternalMapState<K, N, UK, UV> original,
            LatencyTrackingMapStateMetrics latencyTrackingStateMetric) {
        super(original, latencyTrackingStateMetric);
    }

    @Override
    public UV get(UK key) throws Exception {
        return null;
    }

    @Override
    public void put(UK key, UV value) throws Exception {}

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {}

    @Override
    public void remove(UK key) throws Exception {}

    @Override
    public boolean contains(UK key) throws Exception {
        return false;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        return null;
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        return null;
    }

    @Override
    public Iterable<UV> values() throws Exception {
        return null;
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        return null;
    }

    @Override
    public boolean isEmpty() throws Exception {
        return false;
    }

    protected static class LatencyTrackingMapStateMetrics
            extends AbstractLatencyTrackingStateMetric {
        static final String MAP_STATE_GET_LATENCY = "mapStateGetLatency";
        static final String MAP_STATE_PUT_LATENCY = "mapStatePutLatency";
        static final String MAP_STATE_PUT_ALL_LATENCY = "mapStatePutAllLatency";
        static final String MAP_STATE_CONTAINS_LATENCY = "mapStateContainsAllLatency";
        static final String MAP_STATE_ENTRIES_LATENCY = "mapStateEntriesLatency";
        static final String MAP_STATE_KEYS_LATENCY = "mapStateKeysLatency";
        static final String MAP_STATE_VALUES_LATENCY = "mapStateValuesLatency";
        static final String MAP_STATE_ITERATOR_LATENCY = "mapStateIteratorLatency";
        static final String MAP_STATE_IS_EMPTY_LATENCY = "mapStateIsEmptyLatency";
        static final String MAP_STATE_CLEAR_LATENCY = "mapStateClearLatency";

        LatencyTrackingMapStateMetrics(
                String stateName, MetricGroup metricGroup, int sampleInterval, long slidingWindow) {
            super(stateName, metricGroup, sampleInterval, slidingWindow);
        }

        boolean checkGetCounter() {
            return checkCounter(MAP_STATE_GET_LATENCY);
        }

        boolean checkPutCounter() {
            return checkCounter(MAP_STATE_PUT_LATENCY);
        }
    }
}
