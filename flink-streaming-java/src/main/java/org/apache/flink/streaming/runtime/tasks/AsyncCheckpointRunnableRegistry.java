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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link CloseableRegistry} with {@link AsyncCheckpointRunnable}s.
 */
@Internal
public class AsyncCheckpointRunnableRegistry extends CloseableRegistry {
	private final Map<Long, AsyncCheckpointRunnable> checkpoints;

	public AsyncCheckpointRunnableRegistry() {
		super();
		this.checkpoints = new HashMap<>();
	}

	public void registerAsyncCheckpointRunnable(long checkpointId, AsyncCheckpointRunnable asyncCheckpointRunnable) throws IOException {
		synchronized (getSynchronizationLock()) {
			registerCloseable(asyncCheckpointRunnable);
			checkpoints.put(checkpointId, asyncCheckpointRunnable);
		}
	}

	public boolean unregisterAsyncCheckpointRunnable(long checkpointId) {
		synchronized (getSynchronizationLock()) {
			AsyncCheckpointRunnable asyncCheckpointRunnable = checkpoints.remove(checkpointId);
			return unregisterCloseable(asyncCheckpointRunnable);
		}
	}

	/**
	 * Cancel the async checkpoint runnable with given checkpoint id.
	 * If given checkpoint id is not registered, return false, otherwise return true.
	 */
	public boolean cancelAsyncCheckpointRunnable(long checkpointId) {
		AsyncCheckpointRunnable asyncCheckpointRunnable;
		synchronized (getSynchronizationLock()) {
			asyncCheckpointRunnable = checkpoints.remove(checkpointId);
			unregisterCloseable(asyncCheckpointRunnable);
		}
		IOUtils.closeQuietly(asyncCheckpointRunnable);
		return asyncCheckpointRunnable != null;
	}

	@VisibleForTesting
	public Map<Long, AsyncCheckpointRunnable> getAsyncCheckpointOperations() {
		return Collections.unmodifiableMap(checkpoints);
	}

	@Override
	public void close() throws IOException {
		super.close();
		checkpoints.clear();
	}
}
