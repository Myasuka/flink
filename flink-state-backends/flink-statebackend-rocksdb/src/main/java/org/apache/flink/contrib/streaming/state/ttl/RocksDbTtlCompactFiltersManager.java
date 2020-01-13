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

package org.apache.flink.contrib.streaming.state.ttl;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.rocksdb.ColumnFamilyOptions;

import javax.annotation.Nonnull;

/** RocksDB compaction filter utils for state with TTL. */
public class RocksDbTtlCompactFiltersManager {

	/** Enables RocksDb compaction filter for State with TTL. */
	private final boolean enableTtlCompactionFilter;

	private final TtlTimeProvider ttlTimeProvider;

	/** Registered compaction filter factories. */

	public RocksDbTtlCompactFiltersManager(boolean enableTtlCompactionFilter, TtlTimeProvider ttlTimeProvider) {
		this.enableTtlCompactionFilter = enableTtlCompactionFilter;
		this.ttlTimeProvider = ttlTimeProvider;
	}

	public void setAndRegisterCompactFilterIfStateTtl(
		@Nonnull RegisteredStateMetaInfoBase metaInfoBase,
		@Nonnull ColumnFamilyOptions options) {

		if (enableTtlCompactionFilter && metaInfoBase instanceof RegisteredKeyValueStateBackendMetaInfo) {
			RegisteredKeyValueStateBackendMetaInfo kvMetaInfoBase = (RegisteredKeyValueStateBackendMetaInfo) metaInfoBase;
			if (TtlStateFactory.TtlSerializer.isTtlStateSerializer(kvMetaInfoBase.getStateSerializer())) {
				createAndSetCompactFilterFactory(metaInfoBase.getName(), options);
			}
		}
	}

	private void createAndSetCompactFilterFactory(String stateName, @Nonnull ColumnFamilyOptions options) {
	}

	private static org.rocksdb.Logger createRocksDbNativeLogger() {
		return null;
	}

	public void configCompactFilter(
			@Nonnull StateDescriptor<?, ?> stateDesc,
			TypeSerializer<?> stateSerializer) {
	}

	public void disposeAndClearRegisteredCompactionFactories() {
	}
}
