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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.OutputStreamAndPath;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link CheckpointStreamFactory} that supports to create different streams on the same underlying file.
 */
public class FsSegmentCheckpointStreamFactory implements CheckpointStreamFactory {
	private static final Logger LOG = LoggerFactory.getLogger(FsSegmentCheckpointStreamFactory.class);

	/** Default size for the write buffer. */
	private static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

	/**
	 * FileSegmentCheckpointStreamFactory-wide lock to safeguard the output stream updates.
	 */
	private final Object lock;

	/**
	 * Cached handle to the file system for file operations.
	 */
	private final FileSystem filesystem;

	/** The directory for shared checkpoint data. */
	private final Path sharedStateDirectory;

	/** The directory for taskowned data. */
	private final Path taskOwnedStateDirectory;

	/** The directory for checkpoint exclusive state data. */
	private final Path checkpointDirector;

	private final int writeBufferSize;

	/** The currentCheckpointOutputStream of the current output stream. */
	private FsSegmentCheckpointStateOutputStream currentCheckpointOutputStream;

	/** The current output stream for the checkpoint data. */
	private FSDataOutputStream currentFileOutputStream;

	/** The path of the file under writing. */
	private Path currentFilePath;

	/** Will be used when delivery to FsCheckpointStateOutputStream. */
	private final int fileSizeThreshold;

	FsSegmentCheckpointStreamFactory(
		FileSystem fileSystem,
		Path checkpointDirectory,
		Path sharedStateDirectory,
		Path taskOwnedStateDirectory,
		int fileSizeThreshold) {
		this(fileSystem, checkpointDirectory, sharedStateDirectory, taskOwnedStateDirectory, DEFAULT_WRITE_BUFFER_SIZE, fileSizeThreshold);
	}

	FsSegmentCheckpointStreamFactory(
		FileSystem fileSystem,
		Path checkpointDirector,
		Path sharedStateDirectory,
		Path taskOwnedStateDirectory,
		int writeBufferSize,
		int fileSizeThreshold) {
		this.filesystem = checkNotNull(fileSystem);
		this.checkpointDirector = checkNotNull(checkpointDirector);
		this.sharedStateDirectory = checkNotNull(sharedStateDirectory);
		this.taskOwnedStateDirectory = checkNotNull(taskOwnedStateDirectory);
		this.writeBufferSize = writeBufferSize;
		this.fileSizeThreshold = fileSizeThreshold;
		this.lock = new Object();
	}

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(
		long checkpointId,
		CheckpointedStateScope scope) throws IOException {
		// delivery to FsCheckpointStateOutputStream for EXCLUSIVE scope
		if (CheckpointedStateScope.EXCLUSIVE.equals(scope)) {
			return new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
				checkpointDirector,
				filesystem,
				writeBufferSize,
				fileSizeThreshold);
		}
		synchronized (lock) {
			if (currentCheckpointOutputStream != null) {
				throw new UnsupportedOperationException("Do not support concurrent streams.");
			}

			try {
				this.currentCheckpointOutputStream = new FsSegmentCheckpointStateOutputStream(checkpointId, sharedStateDirectory);
				return this.currentCheckpointOutputStream;
			} catch (IOException e) {
				closeFileOutputStream();
				throw e;
			}
		}
	}

	@Override
	public void dispose() {
		closeFileOutputStream();
	}

	@VisibleForTesting
	void closeFileOutputStream() {
		// currently we reuse a single underlying file for a checkpoint,
		// we need to close the underlying file when the checkpoint stream factory disposed.
		try {
			if (currentFileOutputStream != null) {
				currentFileOutputStream.close();
			}
		} catch (IOException e) {
			LOG.warn("Can not close the checkpoint file.", e);
		} finally {
			currentCheckpointOutputStream = null;
			currentFileOutputStream = null;
		}
	}

	public Path getSharedStateDirectory() {
		return sharedStateDirectory;
	}

	public Path getTaskOwnedStateDirectory() {
		return taskOwnedStateDirectory;
	}

	public FileSystem getFileSystem() {
		return filesystem;
	}

	@VisibleForTesting
	FSDataOutputStream getFileOutputStream() {
		return currentFileOutputStream;
	}

	private Path createStatePath(long checkpointId, Path basePath) {
		return new Path(basePath, "chk-" + checkpointId + "-" + UUID.randomUUID().toString());
	}

	private void createFileOutputStream(long checkpointId, Path basePath) throws IOException {
		Exception latestException = null;
		for (int attempt = 0; attempt < 10; attempt++) {
			try {
				Path nextFilePath = createStatePath(checkpointId, basePath);

				OutputStreamAndPath streamAndPath = EntropyInjector.createEntropyAware(
					filesystem, nextFilePath, FileSystem.WriteMode.NO_OVERWRITE);

				currentFilePath = streamAndPath.path();
				currentFileOutputStream = streamAndPath.stream();
				return;
			} catch (Exception e) {
				latestException = e;
			}
		}

		throw new IOException("Could not open output stream for state backend", latestException);
	}

	/**
	 * A {@link org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream} will reuse the underlying file if possible.
	 */
	public final class FsSegmentCheckpointStateOutputStream extends CheckpointStreamFactory.CheckpointStateOutputStream {
		/** Base path for current checkpoint. */
		private final Path checkpointBasePath;

		/** Current checkpoint ID. */
		private final long checkpointId;
		/**
		 * The owner thread of the output stream.
		 */
		private final long ownerThreadID;

		/** Buffer contains the checkpoint data before flush to file. */
		private final byte[] buffer;

		/** Index of buffer which will be written into. */
		private int bufferIndex;

		/**
		 * start position of the underlying file which current output stream will write to.
		 */
		private final long startPos;

		/** Whether current output stream is closed or not. */
		private boolean closed;

		FsSegmentCheckpointStateOutputStream(long checkpointId, Path basePath) throws IOException {
			this.checkpointId = checkpointId;
			this.checkpointBasePath = checkNotNull(basePath);
			this.ownerThreadID = Thread.currentThread().getId();

			this.closed = false;

			this.buffer = new byte[writeBufferSize];
			this.bufferIndex = 0;

			this.startPos = currentFileOutputStream == null ? 0 : currentFileOutputStream.getPos();
		}

		@Nullable
		@Override
		public StreamStateHandle closeAndGetHandle() throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);

			synchronized (lock) {
				checkState(!closed);
				Preconditions.checkState(currentCheckpointOutputStream == this);

				try {
					flush();
					long endPos = currentFileOutputStream.getPos();
					return new FsSegmentStateHandle(currentFilePath, startPos, endPos);
				} finally {
					closed = true;
					currentCheckpointOutputStream = null;
				}
			}
		}

		@Override
		public void close() {
			if (closed) {
				return;
			}

			synchronized (lock) {
				try {
					closeFileOutputStream();
				} finally {
					closed = true;
					currentFileOutputStream = null;
				}
			}
		}

		/**
		 * Checks whether the stream is closed.
		 *
		 * @return True if the stream was closed, false if it is still open.
		 */
		public boolean isClosed() {
			return closed;
		}

		@Override
		public long getPos() throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);
			checkState(!closed);

			if (currentFileOutputStream == null) {
				return bufferIndex;
			} else {
				synchronized (lock) {
					checkState(!closed);
					checkState(currentCheckpointOutputStream == this);

					try {
						return bufferIndex + currentFileOutputStream.getPos() - startPos;
					} catch (IOException e) {
						closeFileOutputStream();
						throw e;
					}
				}
			}
		}

		@Override
		public void flush() throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);

			synchronized (lock) {
				checkState(!closed);
				Preconditions.checkState(currentCheckpointOutputStream == this);

				// create a new file if there does not exist an opened one
				if (currentFileOutputStream == null && bufferIndex > 0) {
					createFileOutputStream(checkpointId, checkpointBasePath);
				}

				// flush the data in the buffer to the file.
				if (bufferIndex > 0) {
					try {
						currentFileOutputStream.write(buffer, 0, bufferIndex);
						currentFileOutputStream.flush();

						bufferIndex = 0;
					} catch (IOException e) {
						closeFileOutputStream();
						throw e;
					}
				}
			}
		}

		@Override
		public void sync() throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);

			synchronized (lock) {
				checkState(!closed);
				Preconditions.checkState(currentCheckpointOutputStream == this);

				try {
					currentFileOutputStream.sync();
				} catch (Exception e) {
					closeFileOutputStream();
					throw e;
				}
			}
		}

		@Override
		public void write(int b) throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);
			checkState(!closed);

			if (bufferIndex >= buffer.length) {
				flush();
			}

			if (!closed) {
				buffer[bufferIndex++] = (byte) b;
			}
		}
	}
}

