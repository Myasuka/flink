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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Base class of state latency metric which counts and histogram the state metric. */
class StateLatencyMetricBase implements AutoCloseable {
    protected static final String STATE_CLEAR_LATENCY = "stateClearLatency";
    private final MetricGroup metricGroup;
    private final int sampleInterval;
    private final Map<String, Histogram> histogramMetrics;
    private final Map<String, Counter> countersPerMetric;
    private final Supplier<Histogram> histogramSupplier;

    StateLatencyMetricBase(
            String stateName, MetricGroup metricGroup, int sampleInterval, int historySize) {
        this.metricGroup = metricGroup.addGroup(stateName);
        this.sampleInterval = sampleInterval;
        this.histogramMetrics = new HashMap<>();
        this.countersPerMetric = new HashMap<>();
        this.histogramSupplier = () -> new DescriptiveStatisticsHistogram(historySize);
    }

    protected boolean checkCounter(final String metricName) {
        return countersPerMetric
                .computeIfAbsent(metricName, (k) -> new Counter(sampleInterval))
                .checkAndUpdateCounter();
    }

    protected boolean checkClearCounter() {
        return checkCounter(STATE_CLEAR_LATENCY);
    }

    protected void updateHistogram(final String metricName, final long durationNanoTime) {
        this.histogramMetrics
                .computeIfAbsent(
                        metricName,
                        (k) -> {
                            Histogram histogram = histogramSupplier.get();
                            metricGroup.histogram(metricName, histogram);
                            return histogram;
                        })
                .update(durationNanoTime);
    }

    protected void updateClearLatency(long duration) {
        updateHistogram(STATE_CLEAR_LATENCY, duration);
    }

    @VisibleForTesting
    Map<String, Counter> getCountersPerMetric() {
        return countersPerMetric;
    }

    @Override
    public void close() throws Exception {
        histogramMetrics.clear();
        countersPerMetric.clear();
    }

    protected static class Counter {
        private final int metricSampledInterval;
        private int counter;

        Counter(int metricSampledInterval) {
            this.metricSampledInterval = metricSampledInterval;
            this.counter = 0;
        }

        private int updateMetricsSampledCounter(int counter) {
            return (counter + 1 < metricSampledInterval) ? counter + 1 : 0;
        }

        boolean checkAndUpdateCounter() {
            boolean result = counter == 0;
            this.counter = updateMetricsSampledCounter(counter);
            return result;
        }

        @VisibleForTesting
        int getCounter() {
            return counter;
        }

        @VisibleForTesting
        void resetCounter() {
            this.counter = 0;
        }
    }
}
