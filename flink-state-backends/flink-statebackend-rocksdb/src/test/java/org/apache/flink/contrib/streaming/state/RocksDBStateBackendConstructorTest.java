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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.filesystem.AbstractFileStateBackend;
import org.apache.flink.runtime.state.filesystem.FsSegmentStateBackend;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link RocksDBStateBackend} with new constructor.
 */
public class RocksDBStateBackendConstructorTest extends TestLogger {
	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	// ------------------------------------------------------------------------

	/**
	 * Validates taking the application-defined file system state backend and adding with additional
	 * parameters from the cluster configuration, but giving precedence to application-defined
	 * parameters over configuration-defined parameters.
	 */
	@Test
	public void testLoadFileSystemStateBackendMixed() throws Exception {
		final String appCheckpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();

		final String localDir1 = tmp.newFolder().getAbsolutePath();
		final String localDir2 = tmp.newFolder().getAbsolutePath();
		final String localDir3 = tmp.newFolder().getAbsolutePath();
		final String localDir4 = tmp.newFolder().getAbsolutePath();

		final boolean incremental = !CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue();

		final Path expectedCheckpointsPath = new Path(appCheckpointDir);
		final Path expectedSavepointsPath = new Path(savepointDir);

		final RocksDBStateBackend backend = new RocksDBStateBackend(appCheckpointDir, incremental, true);
		assertTrue(backend.getCheckpointBackend() instanceof FsSegmentStateBackend);
		backend.setDbStoragePaths(localDir1, localDir2);

		final Configuration config = new Configuration();
		config.setString(CheckpointingOptions.STATE_BACKEND, "jobmanager"); // this should not be picked up
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir); // this should not be picked up
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, !incremental);  // this should not be picked up
		config.setString(RocksDBOptions.LOCAL_DIRECTORIES, localDir3 + ":" + localDir4);  // this should not be picked up

		final StateBackend loadedBackend =
			StateBackendLoader.fromApplicationOrConfigOrDefault(backend, config, Thread.currentThread().getContextClassLoader(), null);
		assertTrue(loadedBackend instanceof RocksDBStateBackend);

		final RocksDBStateBackend loadedRocks = (RocksDBStateBackend) loadedBackend;

		assertEquals(incremental, loadedRocks.isIncrementalCheckpointsEnabled());
		checkPaths(loadedRocks.getDbStoragePaths(), localDir1, localDir2);

		AbstractFileStateBackend fsBackend = (AbstractFileStateBackend) loadedRocks.getCheckpointBackend();
		assertTrue(fsBackend instanceof FsSegmentStateBackend);
		assertEquals(expectedCheckpointsPath, fsBackend.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fsBackend.getSavepointPath());
	}

	// ------------------------------------------------------------------------

	private static void checkPaths(String[] pathsArray, String... paths) {
		assertNotNull(pathsArray);
		assertNotNull(paths);

		assertEquals(pathsArray.length, paths.length);

		HashSet<String> pathsSet = new HashSet<>(Arrays.asList(pathsArray));

		for (String path : paths) {
			assertTrue(pathsSet.contains(path));
		}
	}
}

