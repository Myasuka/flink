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

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.runtime.state.metrics.LatencyTrackingValueState.ValueStateLatencyMetrics.VALUE_STATE_GET_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingValueState.ValueStateLatencyMetrics.VALUE_STATE_UPDATE_LATENCY;
import static org.hamcrest.core.Is.is;

/** Tests for {@link LatencyTrackingValueState}. */
public class LatencyTrackingValueStateTest extends LatencyTrackingStateTestBase<Integer> {
    @Override
    @SuppressWarnings("unchecked")
    ValueStateDescriptor<Long> getStateDescriptor() {
        return new ValueStateDescriptor<>("value", Long.class);
    }

    @Override
    IntSerializer getKeySerializer() {
        return IntSerializer.INSTANCE;
    }

    @Override
    void setCurrentKey(AbstractKeyedStateBackend<Integer> keyedBackend) {
        keyedBackend.setCurrentKey(1);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testLatencyTrackingValueState() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            LatencyTrackingValueState<Integer, VoidNamespace, Long> latencyTrackingState =
                    (LatencyTrackingValueState)
                            createLatencyTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            StateLatencyMetricBase latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();
            Map<String, StateLatencyMetricBase.Counter> countersPerMetric =
                    latencyTrackingStateMetric.getCountersPerMetric();
            Assert.assertThat(countersPerMetric.isEmpty(), is(true));
            setCurrentKey(keyedBackend);
            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
                latencyTrackingState.update(ThreadLocalRandom.current().nextLong());
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric.get(VALUE_STATE_UPDATE_LATENCY).getCounter());
                latencyTrackingState.value();
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric.get(VALUE_STATE_GET_LATENCY).getCounter());
            }
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }
}
