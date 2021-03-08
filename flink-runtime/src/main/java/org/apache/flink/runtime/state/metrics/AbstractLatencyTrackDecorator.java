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

import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import java.io.IOException;
import java.util.function.Consumer;

public class AbstractLatencyTrackDecorator<S> {
    protected S original;

    AbstractLatencyTrackDecorator(S original) {
        this.original = original;
    }

    protected <T> T trackLatencyWithIOException(
            SupplierWithException<T, IOException> supplier, Consumer<Long> consumer)
            throws IOException {
        long startTime = System.nanoTime();
        T result = supplier.get();
        long latency = System.nanoTime() - startTime;
        consumer.accept(latency);
        return result;
    }

    protected <T> T trackLatencyWithException(
            SupplierWithException<T, Exception> supplier, Consumer<Long> consumer)
            throws Exception {
        long startTime = System.nanoTime();
        T result = supplier.get();
        long latency = System.nanoTime() - startTime;
        consumer.accept(latency);
        return result;
    }

    protected void trackLatencyWithIOException(
            ThrowingRunnable<IOException> runnable, Consumer<Long> consumer) throws IOException {
        long startTime = System.nanoTime();
        runnable.run();
        long latency = System.nanoTime() - startTime;
        consumer.accept(latency);
    }

    protected void trackLatencyWithException(
            ThrowingRunnable<Exception> runnable, Consumer<Long> consumer) throws Exception {
        long startTime = System.nanoTime();
        runnable.run();
        long latency = System.nanoTime() - startTime;
        consumer.accept(latency);
    }

    protected void trackLatency(Runnable runnable, Consumer<Long> consumer) {
        long startTime = System.nanoTime();
        runnable.run();
        long latency = System.nanoTime() - startTime;
        consumer.accept(latency);
    }
}
