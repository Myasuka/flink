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
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

/** RocksDB compaction filter utils for state with TTL. */
public class RocksDbTtlCompactFiltersManager {
    private static final Logger LOG =
            LoggerFactory.getLogger(RocksDbTtlCompactFiltersManager.class);

    private final TtlTimeProvider ttlTimeProvider;

    public RocksDbTtlCompactFiltersManager(TtlTimeProvider ttlTimeProvider) {
        this.ttlTimeProvider = ttlTimeProvider;
    }

    public void setAndRegisterCompactFilterIfStateTtl(
            @Nonnull RegisteredStateMetaInfoBase metaInfoBase,
            @Nonnull ColumnFamilyOptions options) {

        if (metaInfoBase instanceof RegisteredKeyValueStateBackendMetaInfo) {
            RegisteredKeyValueStateBackendMetaInfo kvMetaInfoBase =
                    (RegisteredKeyValueStateBackendMetaInfo) metaInfoBase;
            if (TtlStateFactory.TtlSerializer.isTtlStateSerializer(
                    kvMetaInfoBase.getStateSerializer())) {
                createAndSetCompactFilterFactory(metaInfoBase.getName(), options);
            }
        }
    }

    private void createAndSetCompactFilterFactory(
            String stateName, @Nonnull ColumnFamilyOptions options) {}

    private static org.rocksdb.Logger createRocksDbNativeLogger() {
        if (LOG.isDebugEnabled()) {
            // options are always needed for org.rocksdb.Logger construction (no other constructor)
            // the logger level gets configured from the options in native code
            try (DBOptions opts = new DBOptions().setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL)) {
                return new org.rocksdb.Logger(opts) {
                    @Override
                    protected void log(InfoLogLevel infoLogLevel, String logMsg) {
                        LOG.debug("RocksDB filter native code log: " + logMsg);
                    }
                };
            }
        } else {
            return null;
        }
    }

    public void configCompactFilter(
            @Nonnull StateDescriptor<?, ?> stateDesc, TypeSerializer<?> stateSerializer) {
        StateTtlConfig ttlConfig = stateDesc.getTtlConfig();
    }

    public void disposeAndClearRegisteredCompactionFactories() {}
}
