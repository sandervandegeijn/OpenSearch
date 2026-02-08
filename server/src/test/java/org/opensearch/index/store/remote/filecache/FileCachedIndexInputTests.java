/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class FileCachedIndexInputTests extends OpenSearchTestCase {

    protected FileCache fileCache;
    protected Path filePath;
    protected IndexInput underlyingIndexInput;
    protected FileCachedIndexInput fileCachedIndexInput;

    protected static final int FILE_CACHE_CAPACITY = 1000;
    protected static final String TEST_FILE = "test_file";
    protected static final String SLICE_DESC = "slice_description";

    @Before
    public void setup() throws IOException {
        Path basePath = createTempDir("FileCachedIndexInputTests");
        FSDirectory fsDirectory = FSDirectory.open(basePath);
        IndexOutput indexOutput = fsDirectory.createOutput(TEST_FILE, IOContext.DEFAULT);
        // Writing to the file so that it's size is not zero
        indexOutput.writeInt(100);
        indexOutput.close();
        filePath = basePath.resolve(TEST_FILE);
        underlyingIndexInput = fsDirectory.openInput(TEST_FILE, IOContext.DEFAULT);
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(FILE_CACHE_CAPACITY);
    }

    protected void setupIndexInputAndAddToFileCache() {
        fileCachedIndexInput = new FileCachedIndexInput(fileCache, filePath, underlyingIndexInput);
        fileCache.put(filePath, new CachedFullFileIndexInput(fileCache, filePath, fileCachedIndexInput));
    }

    public void testClone() throws IOException {
        setupIndexInputAndAddToFileCache();

        // Since the file ia already in cache and has refCount 1, activeUsage and totalUsage will be same
        assertTrue(isActiveAndTotalUsageSame());

        // Decrementing the refCount explicitly on the file which will make it inactive (as refCount will drop to 0)
        fileCache.decRef(filePath);
        assertFalse(isActiveAndTotalUsageSame());

        // After cloning the refCount will increase again and activeUsage and totalUsage will be same again
        FileCachedIndexInput clonedFileCachedIndexInput = fileCachedIndexInput.clone();
        assertTrue(isActiveAndTotalUsageSame());

        // Closing the clone will again decrease the refCount making it 0
        clonedFileCachedIndexInput.close();
        assertFalse(isActiveAndTotalUsageSame());
    }

    public void testSlice() throws IOException {
        setupIndexInputAndAddToFileCache();
        assertThrows(UnsupportedOperationException.class, () -> fileCachedIndexInput.slice(SLICE_DESC, 10, 100));
    }

    public void testCleanerDecRefsOnGC() {
        setupIndexInputAndAddToFileCache();

        // Drop the put refCount so it starts at 0
        fileCache.decRef(filePath);
        assertEquals(0, (int) fileCache.getRef(filePath));

        // Create a clone (refCount becomes 1), then drop the reference without closing
        fileCachedIndexInput.clone();
        assertEquals(1, (int) fileCache.getRef(filePath));

        // Trigger GC and assert the Cleaner decrements refCount back to 0
        try {
            assertBusy(() -> {
                System.gc();
                assertEquals("Expected refCount to drop to zero after GC cleans up unclosed clone", 0, (int) fileCache.getRef(filePath));
            }, 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Exception thrown while triggering gc", e);
            fail("GC-based cleaner test failed: " + e.getMessage());
        }
    }

    public void testCloseAndCleanerIdempotent() throws IOException {
        setupIndexInputAndAddToFileCache();

        // Drop the put refCount so it starts at 0
        fileCache.decRef(filePath);

        // Create 3 clones
        FileCachedIndexInput clone1 = fileCachedIndexInput.clone();
        FileCachedIndexInput clone2 = fileCachedIndexInput.clone();
        FileCachedIndexInput clone3 = fileCachedIndexInput.clone();
        assertEquals(3, (int) fileCache.getRef(filePath));

        // Close clone1, then invoke cleaner — refCount should only decrement once
        clone1.close();
        assertEquals(2, (int) fileCache.getRef(filePath));
        clone1.indexInputHolderRun();
        assertEquals(2, (int) fileCache.getRef(filePath));

        // Invoke cleaner on clone2 first, then close — refCount should only decrement once
        clone2.indexInputHolderRun();
        assertEquals(1, (int) fileCache.getRef(filePath));
        clone2.close();
        assertEquals(1, (int) fileCache.getRef(filePath));

        // Close clone3, then invoke cleaner
        clone3.close();
        assertEquals(0, (int) fileCache.getRef(filePath));
        clone3.indexInputHolderRun();
        assertEquals(0, (int) fileCache.getRef(filePath));
    }

    public void testMultipleUnclosedClones() {
        setupIndexInputAndAddToFileCache();

        // Drop the put refCount so it starts at 0
        fileCache.decRef(filePath);
        assertEquals(0, (int) fileCache.getRef(filePath));

        int numClones = 5;
        for (int i = 0; i < numClones; i++) {
            fileCachedIndexInput.clone();
        }
        assertEquals(numClones, (int) fileCache.getRef(filePath));

        // Trigger GC and assert all clones get cleaned up
        try {
            assertBusy(() -> {
                System.gc();
                assertEquals(
                    "Expected refCount to drop to zero after GC cleans up all unclosed clones",
                    0,
                    (int) fileCache.getRef(filePath)
                );
            }, 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Exception thrown while triggering gc", e);
            fail("GC-based cleaner test failed: " + e.getMessage());
        }
    }

    public void testCloneAfterCloseThrows() throws IOException {
        setupIndexInputAndAddToFileCache();
        fileCachedIndexInput.close();
        assertThrows(AlreadyClosedException.class, () -> fileCachedIndexInput.clone());
    }

    protected boolean isActiveAndTotalUsageSame() {
        return fileCache.activeUsage() == fileCache.usage();
    }
}
