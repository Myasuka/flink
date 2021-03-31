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

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.MapStateLatencyMetrics.MAP_STATE_ITERATOR_HAS_NEXT_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.MapStateLatencyMetrics.MAP_STATE_ITERATOR_NEXT_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.MapStateLatencyMetrics.MAP_STATE_ITERATOR_REMOVE_LATENCY;

/** Tests for {@link LatencyTrackingMapState}. */
public class LatencyTrackingMapStateTest extends LatencyTrackingStateTestBase<Integer> {
    @Override
    @SuppressWarnings("unchecked")
    MapStateDescriptor<Integer, Double> getStateDescriptor() {
        return new MapStateDescriptor<>("map", Integer.class, Double.class);
    }

    @Override
    TypeSerializer<Integer> getKeySerializer() {
        return IntSerializer.INSTANCE;
    }

    @Override
    void setCurrentKey(AbstractKeyedStateBackend<Integer> keyedBackend) {
        keyedBackend.setCurrentKey(1);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testLatencyTrackingMapState() throws Exception {
        //        AbstractKeyedStateBackend<Integer> keyedBackend =
        // createKeyedBackend(getKeySerializer());
        //        try {
        //            LatencyTrackingMapState<Integer, VoidNamespace, Long, Double>
        // latencyTrackingState =
        //                    (LatencyTrackingMapState)
        //                            createLatencyTrackingState(keyedBackend,
        // getStateDescriptor());
        //            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
        //            StateLatencyMetricBase latencyTrackingStateMetric =
        //                    latencyTrackingState.getLatencyTrackingStateMetric();
        //            Map<String, StateLatencyMetricBase.Counter> countersPerMetric =
        //                    latencyTrackingStateMetric.getCountersPerMetric();
        //            Assert.assertThat(countersPerMetric.isEmpty(), is(true));
        //            setCurrentKey(keyedBackend);
        //            ThreadLocalRandom random = ThreadLocalRandom.current();
        //            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
        //                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
        //                latencyTrackingState.put(random.nextLong(), random.nextDouble());
        //                Assert.assertEquals(
        //                        expectedResult,
        // countersPerMetric.get(MAP_STATE_PUT_LATENCY).getCounter());
        //                latencyTrackingState.putAll(
        //                        Collections.singletonMap(random.nextLong(), random.nextDouble()));
        //                Assert.assertEquals(
        //                        expectedResult,
        //                        countersPerMetric.get(MAP_STATE_PUT_ALL_LATENCY).getCounter());
        //                latencyTrackingState.get(random.nextLong());
        //                Assert.assertEquals(
        //                        expectedResult,
        // countersPerMetric.get(MAP_STATE_GET_LATENCY).getCounter());
        //                latencyTrackingState.remove(random.nextLong());
        //                Assert.assertEquals(
        //                        expectedResult,
        //                        countersPerMetric.get(MAP_STATE_REMOVE_LATENCY).getCounter());
        //                latencyTrackingState.contains(random.nextLong());
        //                Assert.assertEquals(
        //                        expectedResult,
        //                        countersPerMetric.get(MAP_STATE_CONTAINS_LATENCY).getCounter());
        //                latencyTrackingState.isEmpty();
        //                Assert.assertEquals(
        //                        expectedResult,
        //                        countersPerMetric.get(MAP_STATE_IS_EMPTY_LATENCY).getCounter());
        //                latencyTrackingState.entries();
        //                Assert.assertEquals(
        //                        expectedResult,
        //
        // countersPerMetric.get(MAP_STATE_ENTRIES_INIT_LATENCY).getCounter());
        //                latencyTrackingState.keys();
        //                Assert.assertEquals(
        //                        expectedResult,
        //                        countersPerMetric.get(MAP_STATE_KEYS_INIT_LATENCY).getCounter());
        //                latencyTrackingState.values();
        //                Assert.assertEquals(
        //                        expectedResult,
        //
        // countersPerMetric.get(MAP_STATE_VALUES_INIT_LATENCY).getCounter());
        //                latencyTrackingState.iterator();
        //                Assert.assertEquals(
        //                        expectedResult,
        //
        // countersPerMetric.get(MAP_STATE_ITERATOR_INIT_LATENCY).getCounter());
        //            }
        //        } finally {
        //            if (keyedBackend != null) {
        //                keyedBackend.close();
        //                keyedBackend.dispose();
        //            }
        //        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testLatencyTrackingMapStateIterator() throws Exception {
        //        AbstractKeyedStateBackend<Integer> keyedBackend =
        // createKeyedBackend(getKeySerializer());
        //        try {
        //            LatencyTrackingMapState<Integer, VoidNamespace, Long, Double>
        // latencyTrackingState =
        //                    (LatencyTrackingMapState)
        //                            createLatencyTrackingState(keyedBackend,
        // getStateDescriptor());
        //            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
        //            StateLatencyMetricBase latencyTrackingStateMetric =
        //                    latencyTrackingState.getLatencyTrackingStateMetric();
        //            Map<String, StateLatencyMetricBase.Counter> countersPerMetric =
        //                    latencyTrackingStateMetric.getCountersPerMetric();
        //            setCurrentKey(keyedBackend);
        //
        //            verifyIterator(
        //                    latencyTrackingState, countersPerMetric,
        // latencyTrackingState.iterator(), true);
        //            verifyIterator(
        //                    latencyTrackingState,
        //                    countersPerMetric,
        //                    latencyTrackingState.entries().iterator(),
        //                    true);
        //            verifyIterator(
        //                    latencyTrackingState,
        //                    countersPerMetric,
        //                    latencyTrackingState.keys().iterator(),
        //                    false);
        //            verifyIterator(
        //                    latencyTrackingState,
        //                    countersPerMetric,
        //                    latencyTrackingState.values().iterator(),
        //                    false);
        //        } finally {
        //            if (keyedBackend != null) {
        //                keyedBackend.close();
        //                keyedBackend.dispose();
        //            }
        //        }
    }

    private <E> void verifyIterator(
            LatencyTrackingMapState<Integer, VoidNamespace, Long, Double> latencyTrackingState,
            Map<String, StateLatencyMetricBase.Counter> countersPerMetric,
            Iterator<E> iterator,
            boolean removeIterator)
            throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
            latencyTrackingState.put((long) index, random.nextDouble());
        }
        int count = 1;
        while (iterator.hasNext()) {
            int expectedResult = count == SAMPLE_INTERVAL ? 0 : count;
            Assert.assertEquals(
                    expectedResult,
                    countersPerMetric.get(MAP_STATE_ITERATOR_HAS_NEXT_LATENCY).getCounter());
            iterator.next();
            Assert.assertEquals(
                    expectedResult,
                    countersPerMetric.get(MAP_STATE_ITERATOR_NEXT_LATENCY).getCounter());
            if (removeIterator) {
                iterator.remove();
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric.get(MAP_STATE_ITERATOR_REMOVE_LATENCY).getCounter());
            }
            count += 1;
        }
        // as we call #hasNext on more time than #next, to avoid complex check, just reset hasNext
        // counter in the end.
        countersPerMetric.get(MAP_STATE_ITERATOR_HAS_NEXT_LATENCY).resetCounter();
        latencyTrackingState.clear();
    }
}
