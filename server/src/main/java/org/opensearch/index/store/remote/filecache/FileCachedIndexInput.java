/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reference Counted IndexInput. The first FileCachedIndexInput for a file/block is called origin.
 * origin never references to itself, so the RC = 0 when origin is created.
 * Every time there is a clone to the origin, RC + 1.
 * Every time a clone is closed, RC - 1.
 * When there is an eviction in FileCache, it only cleanups those origins with RC = 0.
 *
 * Since Lucene does not guarantee that it will close clones, a {@link java.lang.ref.Cleaner} is
 * used to handle closing of clones when they become phantom reachable.
 * https://github.com/apache/lucene/blob/8340b01c/lucene/core/src/java/org/apache/lucene/store/IndexInput.java#L32-L33
 *
 * @opensearch.internal
 */
public class FileCachedIndexInput extends IndexInput implements RandomAccessInput {
    private static final Logger logger = LogManager.getLogger(FileCachedIndexInput.class);
    static final Cleaner CLEANER = Cleaner.create(
        OpenSearchExecutors.daemonThreadFactory(AbstractBlockIndexInput.CLEANER_THREAD_NAME_PREFIX)
    );

    protected final FileCache cache;

    /**
     * on disk file path of this index input
     */
    protected Path filePath;

    /**
     * underlying lucene index input which this IndexInput
     * delegate all its read functions to.
     */
    protected IndexInput luceneIndexInput;

    /** indicates if this IndexInput instance is a clone or not */
    protected final boolean isClone;

    protected final AtomicBoolean closed = new AtomicBoolean(false);

    private final IndexInputHolder indexInputHolder;

    public FileCachedIndexInput(FileCache cache, Path filePath, IndexInput underlyingIndexInput) {
        this(cache, filePath, underlyingIndexInput, false);
    }

    FileCachedIndexInput(FileCache cache, Path filePath, IndexInput underlyingIndexInput, boolean isClone) {
        this(cache, filePath, underlyingIndexInput, isClone, true);
    }

    /**
     * Package-private constructor that optionally registers with the Cleaner.
     * Subclasses that manage their own Cleaner (e.g. {@link FullFileCachedIndexInput})
     * should pass {@code registerCleaner = false} to avoid double registration.
     */
    FileCachedIndexInput(FileCache cache, Path filePath, IndexInput underlyingIndexInput, boolean isClone, boolean registerCleaner) {
        super("FileCachedIndexInput (path=" + filePath.toString() + ")");
        this.cache = cache;
        this.filePath = filePath;
        this.luceneIndexInput = underlyingIndexInput;
        this.isClone = isClone;
        if (registerCleaner) {
            this.indexInputHolder = new IndexInputHolder(closed, underlyingIndexInput, isClone, cache, filePath);
            CLEANER.register(this, indexInputHolder);
        } else {
            this.indexInputHolder = null;
        }
    }

    @Override
    public long getFilePointer() {
        return getLuceneIndexInputOrThrow().getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
        getLuceneIndexInputOrThrow().seek(pos);
    }

    @Override
    public long length() {
        return getLuceneIndexInputOrThrow().length();
    }

    @Override
    public byte readByte() throws IOException {
        return getLuceneIndexInputOrThrow().readByte();
    }

    @Override
    public short readShort() throws IOException {
        return getLuceneIndexInputOrThrow().readShort();
    }

    @Override
    public int readInt() throws IOException {
        return getLuceneIndexInputOrThrow().readInt();
    }

    @Override
    public long readLong() throws IOException {
        return getLuceneIndexInputOrThrow().readLong();
    }

    @Override
    public final int readVInt() throws IOException {
        return getLuceneIndexInputOrThrow().readVInt();
    }

    @Override
    public final long readVLong() throws IOException {
        return getLuceneIndexInputOrThrow().readVLong();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        getLuceneIndexInputOrThrow().readBytes(b, offset, len);
    }

    @Override
    public byte readByte(long pos) throws IOException {
        return ((RandomAccessInput) getLuceneIndexInputOrThrow()).readByte(pos);
    }

    @Override
    public short readShort(long pos) throws IOException {
        return ((RandomAccessInput) getLuceneIndexInputOrThrow()).readShort(pos);
    }

    @Override
    public int readInt(long pos) throws IOException {
        return ((RandomAccessInput) getLuceneIndexInputOrThrow()).readInt(pos);
    }

    @Override
    public long readLong(long pos) throws IOException {
        return ((RandomAccessInput) getLuceneIndexInputOrThrow()).readLong(pos);
    }

    @Override
    public FileCachedIndexInput clone() {
        FileCachedIndexInput clonedIndexInput = new FileCachedIndexInput(cache, filePath, getLuceneIndexInputOrThrow().clone(), true);
        cache.incRef(filePath);
        return clonedIndexInput;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        // never reach here!
        throw new UnsupportedOperationException("FileCachedIndexInput couldn't be sliced.");
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            IndexInput toClose = getLuceneIndexInputOrThrow();
            try {
                toClose.close();
            } catch (AlreadyClosedException e) {
                logger.trace("FileCachedIndexInput already closed");
            } catch (IOException e) {
                closed.set(false);
                throw e;
            }
            luceneIndexInput = null;
            if (isClone) {
                cache.decRef(filePath);
            }
        }
    }

    /**
     * Run resource cleaning. To be used only in test.
     */
    void indexInputHolderRun() {
        if (indexInputHolder != null) {
            indexInputHolder.run();
        }
    }

    protected IndexInput getLuceneIndexInputOrThrow() {
        IndexInput input = luceneIndexInput;
        if (input == null) {
            throw new AlreadyClosedException("FileCachedIndexInput is closed");
        }
        return input;
    }

    private static class IndexInputHolder implements Runnable {
        private final AtomicBoolean closed;
        private final IndexInput indexInput;
        private final FileCache cache;
        private final boolean isClone;
        private final Path path;

        IndexInputHolder(AtomicBoolean closed, IndexInput indexInput, boolean isClone, FileCache cache, Path path) {
            this.closed = closed;
            this.indexInput = indexInput;
            this.isClone = isClone;
            this.cache = cache;
            this.path = path;
        }

        @Override
        public void run() {
            if (closed.compareAndSet(false, true)) {
                try {
                    indexInput.close();
                } catch (AlreadyClosedException e) {
                    logger.trace("FileCachedIndexInput already closed by cleaner");
                } catch (IOException e) {
                    closed.set(false);
                    logger.error("Failed to close IndexInput while clearing phantom reachable object", e);
                    return;
                }
                if (isClone) {
                    cache.decRef(path);
                }
            }
        }
    }
}
