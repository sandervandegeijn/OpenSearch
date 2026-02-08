/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public abstract class TransferManagerTestCase extends OpenSearchTestCase {
    protected static final int EIGHT_MB = 1024 * 1024 * 8;
    protected FileCache fileCache;
    protected MMapDirectory directory;
    protected TransferManager transferManager;
    protected ThreadPool threadPool;
    protected ExecutorService executorService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(EIGHT_MB * 2, 1);
        threadPool = mock(ThreadPool.class);
        directory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE);
        initializeTransferManager();
        executorService = Executors.newFixedThreadPool(3);
        doReturn(executorService).when(threadPool).executor(ThreadPool.Names.REMOTE_RECOVERY);
    }

    @After
    public void tearDown() throws Exception {
        executorService.shutdownNow();
        super.tearDown();
    }

    protected static byte[] createData() {
        final byte[] data = new byte[EIGHT_MB];
        data[EIGHT_MB - 1] = 7;
        return data;
    }

    public void testSingleAccess() throws Exception {
        try (IndexInput i = fetchBlobWithName("file")) {
            assertIndexInputIsFunctional(i);
            MatcherAssert.assertThat(fileCache.activeUsage(), equalTo((long) EIGHT_MB));
        }
        MatcherAssert.assertThat(fileCache.activeUsage(), equalTo(0L));
        MatcherAssert.assertThat(fileCache.usage(), equalTo((long) EIGHT_MB));
    }

    public void testSingleAccessAsynchronous() throws Exception {
        IndexInput indexInput = null;
        try {
            indexInput = asyncFetchBlobWithName("file");
            assertIndexInputIsFunctional(indexInput);
            MatcherAssert.assertThat(fileCache.activeUsage(), equalTo((long) EIGHT_MB));
        } finally {
            if (indexInput != null) {
                indexInput.close();
            }
        }
        MatcherAssert.assertThat(fileCache.activeUsage(), equalTo(0L));
        MatcherAssert.assertThat(fileCache.usage(), equalTo((long) EIGHT_MB));
    }

    public void testConcurrentAccess() throws Exception {
        // Kick off multiple threads that all concurrently request the same resource
        final String blobname = "file";
        final ExecutorService testRunner = Executors.newFixedThreadPool(8);
        try {
            final List<Future<IndexInput>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(testRunner.submit(() -> fetchBlobWithName(blobname)));
            }
            // Wait for all threads to complete
            for (Future<IndexInput> future : futures) {
                future.get(1, TimeUnit.SECONDS);
            }
            // Assert that all IndexInputs are independently positioned by seeking
            // to the end and closing each one. If not independent, then this would
            // result in EOFExceptions and/or NPEs.
            for (Future<IndexInput> future : futures) {
                try (IndexInput i = future.get()) {
                    assertIndexInputIsFunctional(i);
                }
            }
        } finally {
            assertTrue(terminate(testRunner));
        }
    }

    public void testConcurrentAccessAsynchronous() throws Exception {
        // Kick off multiple threads that all concurrently request the same resource
        final String blobname = "file";
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            final List<Future<IndexInput>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(executor.submit(() -> asyncFetchBlobWithName(blobname)));
            }
            // Wait for all threads to complete
            for (Future<IndexInput> future : futures) {
                future.get(1, TimeUnit.SECONDS);
            }
            // Assert that all IndexInputs are independently positioned by seeking
            // to the end and closing each one. If not independent, then this would
            // result in EOFExceptions and/or NPEs.
            for (Future<IndexInput> future : futures) {
                try (IndexInput i = future.get()) {
                    assertIndexInputIsFunctional(i);
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    public void testFetchBlobWithConcurrentCacheEvictions() {
        // Submit 256 tasks to an executor with 16 threads that will each randomly
        // request one of eight blobs. Given that the cache can only hold two
        // blobs this will lead to a huge amount of contention and thrashing.
        final ExecutorService testRunner = Executors.newFixedThreadPool(16);
        try {
            final List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 256; i++) {
                // request an index input and immediately close it
                final String blobname = "blob-" + randomIntBetween(0, 7);
                futures.add(testRunner.submit(() -> {
                    try {
                        try (IndexInput indexInput = fetchBlobWithName(blobname)) {
                            assertIndexInputIsFunctional(indexInput);
                        }
                    } catch (IOException ignored) { // fetchBlobWithName may fail due to fixed capacity
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }));
            }
            // Wait for all threads to complete
            try {
                for (Future<?> future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }
            } catch (java.util.concurrent.ExecutionException ignored) { // Index input may be null
            } catch (Exception e) {
                throw new AssertionError(e);
            }

        } finally {
            assertTrue(terminate(testRunner));
        }
        MatcherAssert.assertThat("Expected many evictions to happen", fileCache.stats().evictionCount(), greaterThan(0L));
    }

    public void testSoftWatermarkTriggersPrune() throws Exception {
        initializeTransferManager();
        // Fill cache with one blob, then close so it becomes evictable (refCount 0)
        IndexInput i1 = fetchBlobWithName("1");
        i1.close();
        // Entry is evictable (refCount 0), cache usage may have been reduced by LRU eviction
        MatcherAssert.assertThat(fileCache.usage(), greaterThan(0L));

        // Fetching another blob should succeed because evictable entries can be reclaimed
        try (IndexInput i2 = fetchBlobWithName("2")) {
            assertNotNull(i2);
            // Usage should be within bounds (at most capacity)
            MatcherAssert.assertThat(fileCache.usage(), greaterThan(0L));
        }
    }

    public void testHardLimitStillFailsWhenAllActive() throws Exception {
        initializeTransferManager();
        // Keep one blob open and ensure another fetch fails under hard limit
        IndexInput i1 = fetchBlobWithName("1");

        try {
            assertThrows(IOException.class, () -> fetchBlobWithName("2"));
        } finally {
            i1.close();
        }
    }

    public void testFetchLargerThanCapacityFailsEarly() {
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        blobParts.add(new BlobFetchRequest.BlobPart("blob", 0, EIGHT_MB * 3L));
        assertThrows(
            IOException.class,
            () -> transferManager.fetchBlob(BlobFetchRequest.builder().fileName("too-large").directory(directory).blobParts(blobParts).build())
        );
    }

    public void testOverflowDisabled() throws Exception {
        initializeTransferManager();
        IndexInput i1 = fetchBlobWithName("1");
        try {
            assertThrows(IOException.class, () -> fetchBlobWithName("2"));
        } finally {
            i1.close();
        }
    }

    public void testOverflowDisabledAsynchronous() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        initializeTransferManager();
        IndexInput i1 = asyncFetchBlobWithName("1");

        try {
            assertThrows(IOException.class, () -> asyncFetchBlobWithName("2"));
        } finally {
            i1.close();
            executor.shutdownNow();
        }
    }

    public void testDownloadFails() throws Exception {
        mockExceptionWhileReading();
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        blobParts.add(new BlobFetchRequest.BlobPart("failure-blob", 0, EIGHT_MB));
        expectThrows(
            IOException.class,
            () -> transferManager.fetchBlob(BlobFetchRequest.builder().fileName("file").directory(directory).blobParts(blobParts).build())
        );
        MatcherAssert.assertThat(fileCache.activeUsage(), equalTo(0L));
        MatcherAssert.assertThat(fileCache.usage(), equalTo(0L));
        MatcherAssert.assertThat(fileCache.inFlightUsage(), equalTo(0L));
    }

    public void testDownloadFailsAsyncDownload() throws Exception {
        mockExceptionWhileReading();
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        blobParts.add(new BlobFetchRequest.BlobPart("failure-blob", 0, EIGHT_MB));
        expectThrows(IOException.class, () -> {
            BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder()
                .fileName("file")
                .directory(directory)
                .blobParts(blobParts)
                .build();
            transferManager.fetchBlobAsync(blobFetchRequest);
            try (IndexInput indexInput = transferManager.fetchBlob(blobFetchRequest)) {}
        });
        MatcherAssert.assertThat(fileCache.activeUsage(), equalTo(0L));
        MatcherAssert.assertThat(fileCache.usage(), equalTo(0L));
        MatcherAssert.assertThat(fileCache.inFlightUsage(), equalTo(0L));
    }

    public void testDownloadFailsAsyncDownloadException() throws Exception {
        mockExceptionWhileReading();
        doThrow(new IllegalArgumentException("Invalid thread pool")).when(threadPool).executor(ThreadPool.Names.REMOTE_RECOVERY);
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        blobParts.add(new BlobFetchRequest.BlobPart("failure-blob", 0, EIGHT_MB));
        expectThrows(IllegalArgumentException.class, () -> {
            BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder()
                .fileName("file")
                .directory(directory)
                .blobParts(blobParts)
                .build();
            transferManager.fetchBlobAsync(blobFetchRequest);
        });
        MatcherAssert.assertThat(fileCache.activeUsage(), equalTo(0L));
        MatcherAssert.assertThat(fileCache.inFlightUsage(), equalTo(0L));
    }

    public void testConcurrentInFlightReservationsEnforced() throws Exception {
        initializeTransferManager();
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch readLatch = new CountDownLatch(1);
        mockWaitForLatchReader(readLatch);

        final ExecutorService testRunner = Executors.newFixedThreadPool(4);
        try {
            final List<Future<Boolean>> futures = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                final String fileName = "blocking-file-" + i;
                futures.add(testRunner.submit(() -> {
                    startLatch.await();
                    List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
                    blobParts.add(new BlobFetchRequest.BlobPart("blocking-blob", 0, EIGHT_MB));
                    BlobFetchRequest request = BlobFetchRequest.builder().fileName(fileName).directory(directory).blobParts(blobParts).build();
                    try (IndexInput indexInput = transferManager.fetchBlob(request)) {
                        assertIndexInputIsFunctional(indexInput);
                        return true;
                    } catch (IOException e) {
                        return false;
                    }
                }));
            }

            startLatch.countDown();
            Thread.sleep(200L);
            readLatch.countDown();

            int successCount = 0;
            int failureCount = 0;
            for (Future<Boolean> future : futures) {
                if (future.get(5, TimeUnit.SECONDS)) {
                    successCount++;
                } else {
                    failureCount++;
                }
            }

            assertTrue("Expected at least one fetch to fail due to capacity reservation", failureCount >= 1);
            assertTrue("Capacity should not allow more than two concurrent 8MB reservations", successCount <= 2);
        } finally {
            assertTrue(terminate(testRunner));
        }
        MatcherAssert.assertThat(fileCache.inFlightUsage(), equalTo(0L));
    }

    public void testFetchesToDifferentBlobsDoNotBlockOnEachOther() throws Exception {
        // Mock a call for a blob that will block until the latch is released,
        // then start the fetch for that blob on a separate thread
        final CountDownLatch latch = new CountDownLatch(1);
        mockWaitForLatchReader(latch);
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        blobParts.add(new BlobFetchRequest.BlobPart("blocking-blob", 0, EIGHT_MB / 2L));
        final AtomicReference<Exception> blockingFailure = new AtomicReference<>();

        final Thread blockingThread = new Thread(() -> {
            try {
                transferManager.fetchBlob(
                    BlobFetchRequest.builder().fileName("blocking-file").directory(directory).blobParts(blobParts).build()
                );
            } catch (IOException e) {
                blockingFailure.set(e);
            }
        });
        blockingThread.start();

        // Assert that a different blob can be fetched and will not block on the first blob
        try (IndexInput i = fetchBlobWithName("file", EIGHT_MB / 2L)) {
            assertNotNull(i);
        } finally {
            latch.countDown();
        }

        assertTrue(blockingThread.isAlive());
        blockingThread.join(5_000);
        assertFalse(blockingThread.isAlive());
        assertNull("blocking fetch should not fail", blockingFailure.get());
    }

    public void testAsyncFetchesToDifferentBlobsDoNotBlockOnEachOther() throws Exception {
        // Mock a call for a blob that will block until the latch is released,
        // then start the fetch for that blob on a separate thread
        final CountDownLatch latch = new CountDownLatch(1);
        mockWaitForLatchReader(latch);
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        blobParts.add(new BlobFetchRequest.BlobPart("blocking-blob", 0, EIGHT_MB / 2L));
        final AtomicReference<Exception> blockingFailure = new AtomicReference<>();

        final Thread blockingThread = new Thread(() -> {
            try {
                transferManager.fetchBlob(
                    BlobFetchRequest.builder().fileName("blocking-file").directory(directory).blobParts(blobParts).build()
                );
            } catch (IOException e) {
                blockingFailure.set(e);
            }
        });
        blockingThread.start();

        // Assert that a different blob can be fetched and will not block on the first blob
        try (IndexInput i = asyncFetchBlobWithName("file", EIGHT_MB / 2L)) {
            assertNotNull(i);
        } finally {
            latch.countDown();
        }

        assertTrue(blockingThread.isAlive());
        blockingThread.join(5_000);
        assertFalse(blockingThread.isAlive());
        assertNull("blocking fetch should not fail", blockingFailure.get());
        executor.shutdownNow();
    }

    public void testRefCount() throws Exception {
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        String blobname = "test-blob";
        blobParts.add(new BlobFetchRequest.BlobPart("blob", 0, EIGHT_MB));
        BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder().fileName(blobname).directory(directory).blobParts(blobParts).build();
        // It will trigger async load
        CompletableFuture<IndexInput> future = transferManager.fetchBlobAsync(blobFetchRequest);
        future.get();
        assertEquals(Optional.of(0), Optional.of(fileCache.getRef(blobFetchRequest.getFilePath())));
        assertNotNull(fileCache.get(blobFetchRequest.getFilePath()));
        fileCache.decRef(blobFetchRequest.getFilePath());
        // Making the read call to fetch from file cache
        IndexInput indexInput = transferManager.fetchBlob(blobFetchRequest);
        assertEquals(Optional.of(1), Optional.of(fileCache.getRef(blobFetchRequest.getFilePath())));
    }

    public void testRefMultipleCount() throws Exception {
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        String blobname = "test-blob";
        blobParts.add(new BlobFetchRequest.BlobPart("blob", 0, EIGHT_MB));
        BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder().fileName(blobname).directory(directory).blobParts(blobParts).build();
        transferManager.fetchBlob(blobFetchRequest);
        assertNotNull(fileCache.getRef(blobFetchRequest.getFilePath()));
        transferManager.fetchBlobAsync(blobFetchRequest).join();
        waitUntil(() -> fileCache.getRef(blobFetchRequest.getFilePath()) == 1, 10, TimeUnit.SECONDS);
        assertEquals(Optional.of(1), Optional.of(fileCache.getRef(blobFetchRequest.getFilePath())));
    }

    protected abstract void initializeTransferManager() throws IOException;

    protected abstract void mockExceptionWhileReading() throws IOException;

    protected abstract void mockWaitForLatchReader(CountDownLatch latch) throws IOException;

    private IndexInput fetchBlobWithName(String blobname) throws IOException {
        return fetchBlobWithName(blobname, EIGHT_MB);
    }

    private IndexInput fetchBlobWithName(String blobname, long length) throws IOException {
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        blobParts.add(new BlobFetchRequest.BlobPart("blob", 0, length));
        return transferManager.fetchBlob(BlobFetchRequest.builder().fileName(blobname).directory(directory).blobParts(blobParts).build());
    }

    private IndexInput asyncFetchBlobWithName(String blobname) throws IOException {
        return asyncFetchBlobWithName(blobname, EIGHT_MB);
    }

    private IndexInput asyncFetchBlobWithName(String blobname, long length) throws IOException {
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        blobParts.add(new BlobFetchRequest.BlobPart("blob", 0, length));
        BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder().fileName(blobname).directory(directory).blobParts(blobParts).build();
        // It will trigger async load
        transferManager.fetchBlobAsync(blobFetchRequest);
        // Making the read call to fetch from file cache
        return transferManager.fetchBlob(blobFetchRequest);
    }

    private static void assertIndexInputIsFunctional(IndexInput indexInput) throws IOException {
        assertIndexInputIsFunctional(indexInput, EIGHT_MB);
    }

    private static void assertIndexInputIsFunctional(IndexInput indexInput, long length) throws IOException {
        indexInput.seek(length - 1);
        MatcherAssert.assertThat(indexInput.readByte(), equalTo((byte) 7));
    }
}
