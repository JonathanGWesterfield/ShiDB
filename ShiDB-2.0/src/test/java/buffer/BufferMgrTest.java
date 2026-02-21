package buffer;

import error.BufferAbortException;
import file.BlockId;
import file.FileMgr;
import file.Page;
import org.junit.jupiter.api.*;
import server.ConfigFetcher;
import server.ShiDB;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static server.ConfigFetcher.getConfigs;

public class BufferMgrTest {
    private ShiDB shiDB;
    private BufferMgr bufferMgr;
    private int numInitialBuffers;
    private long bufferMgrUnitTestWaitTime;
    private final String testFileName = "bufferMgrTestFile";

    @BeforeEach
    void setUp() throws IOException {
        // This config shortens the wait timeout for buffer pinning to shorten time for unit testing
        getConfigs("src/test/resources/bufferMgrTestConfig.json");
        bufferMgrUnitTestWaitTime = ConfigFetcher.getBufferMgrMaxWaitTime();
        numInitialBuffers = ConfigFetcher.getSizeOfBufferPool();
        this.shiDB = new ShiDB("BufferMgr-unit-test", ConfigFetcher.getDBFileBlockSize(), numInitialBuffers);
        bufferMgr = shiDB.getBufferMgr();

        BlockId block = new BlockId(testFileName, 0);
        FileMgr fileMgr = shiDB.getFileMgr();
        Page page = new Page(fileMgr.getBlocksize());

        int position1 = 80;
        page.setInt(position1, 345);
        fileMgr.writePageToDisk(block, page);
    }

    @Test
    @DisplayName("Test the pin function")
    public void testPin() {
        Buffer testBuffer = bufferMgr.pinBuffer(new BlockId(testFileName, 0));

        assertEquals(numInitialBuffers - 1, bufferMgr.getNumAvailableBuffers());
        assertTrue(testBuffer.isPinned());
    }

    @Test
    @DisplayName("Test the unpin function")
    public void testUnpin() {
        Buffer testBuffer1 = new Buffer(shiDB.getFileMgr(), shiDB.getLogMgr());

        // Setup up the buffer and bufferMgr to have a single pinned buffer
        // First test that the buffer itself is unpinned
        testBuffer1.pin();
        bufferMgr.unpinBuffer(testBuffer1);

        assertFalse(testBuffer1.isPinned());

        // Now do another setup that tests that the buffer is unPinned AND the num available counter is updated
        bufferMgr = shiDB.getCorrectBufferMgr(null, numInitialBuffers);

        Buffer testBuffer2 = bufferMgr.pinBuffer(new BlockId(testFileName, 0));
        bufferMgr.unpinBuffer(testBuffer2);

        assertFalse(testBuffer2.isPinned());
        assertEquals(numInitialBuffers, bufferMgr.getNumAvailableBuffers());
    }

    @Test
    @DisplayName("Test the timeout functionality when trying to pin on an empty bufferpool")
    public void testPinOnEmptyBufferPoolTimeout() {
        String testFile = "unit_test_file";

        // How much margin to expect before/after timeout value since there is other unit testing overhead
        long timeoutMarginMillis = 150;
        bufferMgr.pinBuffer(new BlockId(testFile, 1));
        bufferMgr.pinBuffer(new BlockId(testFile, 2));
        bufferMgr.pinBuffer(new BlockId(testFile, 3));

        long startTime = System.currentTimeMillis();
        // Since the bufferpool is set to only hold 3 buffers, the next pin should throw an exception
        assertThrows(BufferAbortException.class, () -> bufferMgr.pinBuffer(new BlockId(testFile, 4)));
        long endTime = System.currentTimeMillis();

        // Check that the timeout functionality for the bufferManager works correctly
        long delta = endTime - startTime;
        assertTrue(delta < bufferMgrUnitTestWaitTime + timeoutMarginMillis);
        assertTrue(delta > bufferMgrUnitTestWaitTime - timeoutMarginMillis);
    }

    @Test
    @DisplayName("Test default, naive unpinned buffer strategy")
    public void testNaiveUnpinnedBufferStrategy() {
        // First we pin all the buffers to force an exception on pinning on an empty buffer pool
        // Then we unpin one to ensure that the buffer pool has been freed by 1
        // Then we ensure that the buffer pool layout looks like we expect it to after some additional pins/unpins
        String testFile = "unit_test_file";

        BlockId block1 = new BlockId(testFile, 1);
        BlockId block2 = new BlockId(testFile, 2);
        BlockId block3 = new BlockId(testFile, 3);
        BlockId block4 = new BlockId(testFile, 4);

        Buffer buffer1 = bufferMgr.pinBuffer(block1);
        Buffer buffer2 = bufferMgr.pinBuffer(block2);
        Buffer buffer3 = bufferMgr.pinBuffer(block3);

        assertEquals(0, bufferMgr.getNumAvailableBuffers());
        // Since the bufferpool is set to only hold 3 buffers, the next pin should throw an exception
        assertThrows(BufferAbortException.class, () -> bufferMgr.pinBuffer(block4));

        // Now that we unpin a buffer, pinning should work again
        bufferMgr.unpinBuffer(buffer3);
        Buffer buffer4 = bufferMgr.pinBuffer(block4);

        /* Since this is the naive strategy, this should result in the following mapping:
            buffer1 -> block1
            buffer2 -> block3
            buffer3 -> block4
         */
        bufferMgr.unpinBuffer(buffer4);
        bufferMgr.unpinBuffer(buffer2);
        bufferMgr.pinBuffer(block3);
        bufferMgr.pinBuffer(block4);

        // Check the final allocation of buffers
        assertEquals(block1, buffer1.getBlock());
        assertEquals(block3, buffer2.getBlock());
        assertEquals(block4, buffer3.getBlock());
    }

    @Test
    @DisplayName("Test FIFO unpinned buffer strategy")
    public void testFIFOUnpinnedBufferStrategy() {
        // Refer to testNaiveUnpinnedBufferStrategy() on how this works. This test just does the flavor of making
        // sure that only the least recently pinned block is chosen first
        String testFile = "unit_test_file";
        bufferMgr = shiDB.getCorrectBufferMgr(BufferSelectionStrategy.FIFO, numInitialBuffers);

        // Need all these blocks to force disk reads aka choosing and unused, unpinned block
        // If we pin an existing block, the bufferMgr will just return a buffer that already has the block pinned
        BlockId block1 = new BlockId(testFile, 1);
        BlockId block2 = new BlockId(testFile, 2);
        BlockId block3 = new BlockId(testFile, 3);
        BlockId block4 = new BlockId(testFile, 4);
        BlockId block5 = new BlockId(testFile, 5);
        BlockId block6 = new BlockId(testFile, 6);

        // The buffer variables are meaningless. I just need the references so I can unpin them
        // What matters is the internal bufferPool and what is mapped to what
        Buffer buffer1 = bufferMgr.pinBuffer(block1);
        Buffer buffer2 = bufferMgr.pinBuffer(block2);
        Buffer buffer3 = bufferMgr.pinBuffer(block3);

        // Free element 0, then 1
        bufferMgr.unpinBuffer(buffer1);
        bufferMgr.unpinBuffer(buffer2);

        // should Pin element 0, then element 1
        buffer1 = bufferMgr.pinBuffer(block4);
        buffer2 = bufferMgr.pinBuffer(block5);

        assertEquals(block4, buffer1.getBlock());
        assertEquals(0, buffer1.getPoolIndex());
        assertEquals(block5, buffer2.getBlock());
        assertEquals(1, buffer2.getPoolIndex());

        // unpin element 2, then 1, then 0 -> order of unpinned should make no difference
        bufferMgr.unpinBuffer(buffer3);
        bufferMgr.unpinBuffer(buffer2);
        bufferMgr.unpinBuffer(buffer1);

        // Should pin element 2, then 0, then 1
        buffer1 = bufferMgr.pinBuffer(block6);
        buffer2 =  bufferMgr.pinBuffer(block2);
        buffer3 = bufferMgr.pinBuffer(block1);

        assertEquals(block6, buffer1.getBlock());
        assertEquals(2, buffer1.getPoolIndex());
        assertEquals(block2, buffer2.getBlock());
        assertEquals(0, buffer2.getPoolIndex());
        assertEquals(block1, buffer3.getBlock());
        assertEquals(1, buffer3.getPoolIndex());
    }

    @Test
    @DisplayName("Test LRU unpinned buffer strategy")
    public void testLRUUnpinnedBufferStrategy() {
        // Refer to testNaiveUnpinnedBufferStrategy() on how this works. This test just does the flavor of making
        // sure that only the least recently pinned block is chosen first
        String testFile = "unit_test_file";
        bufferMgr = shiDB.getCorrectBufferMgr(BufferSelectionStrategy.LRU, numInitialBuffers);

        // Need all these blocks to force disk reads aka choosing and unused, unpinned block
        // If we pin an existing block, the bufferMgr will just return a buffer that already has the block pinned
        BlockId block1 = new BlockId(testFile, 1);
        BlockId block2 = new BlockId(testFile, 2);
        BlockId block3 = new BlockId(testFile, 3);
        BlockId block4 = new BlockId(testFile, 4);
        BlockId block5 = new BlockId(testFile, 5);
        BlockId block6 = new BlockId(testFile, 6);


        // The buffer variables are meaningless. I just need the references so I can unpin them
        // What matters is the internal bufferPool and what is mapped to what
        Buffer buffer1 = bufferMgr.pinBuffer(block1);
        Buffer buffer2 = bufferMgr.pinBuffer(block2);
        Buffer buffer3 = bufferMgr.pinBuffer(block3);

        // Free element 1, then 0
        bufferMgr.unpinBuffer(buffer2);
        bufferMgr.unpinBuffer(buffer1);

        // should Pin element 1, then element 0
        buffer1 = bufferMgr.pinBuffer(block4);
        buffer2 = bufferMgr.pinBuffer(block5);

        // Since buffer at index 1 was unpinned first, it should get pinned first
        assertEquals(block4, buffer1.getBlock());
        assertEquals(1, buffer1.getPoolIndex());
        assertEquals(block5, buffer2.getBlock());
        assertEquals(0, buffer2.getPoolIndex());

        // unpin element 2, then 0, then 1
        bufferMgr.unpinBuffer(buffer3);
        bufferMgr.unpinBuffer(buffer2);
        bufferMgr.unpinBuffer(buffer1);

        // Should pin element 2, then 0, then 1
        buffer1 = bufferMgr.pinBuffer(block6);
        buffer2 = bufferMgr.pinBuffer(block2);
        buffer3 = bufferMgr.pinBuffer(block1);

        assertEquals(block6, buffer1.getBlock());
        assertEquals(2, buffer1.getPoolIndex());
        assertEquals(block2, buffer2.getBlock());
        assertEquals(0, buffer2.getPoolIndex());
        assertEquals(block1, buffer3.getBlock());
        assertEquals(1, buffer3.getPoolIndex());
    }
//
//
//    @Test
//    @DisplayName("Test Ring buffer/clock unpinned buffer strategy")
//    public void testClockUnpinnedBufferStrategy() {
//        // Refer to testNaiveUnpinnedBufferStrategy() on how this works. This test just does the flavor of making
//        // sure that only the least recently pinned block is chosen first
//        String testFile = "unit_test_file";
//        bufferMgr.setUnpinnedBufferselectionStrategy(BufferSelectionStrategy.RING_BUFFER);
//
//        // Need all these blocks to force disk reads aka choosing and unused, unpinned block
//        // If we pin an existing block, the bufferMgr will just return a buffer that already has the block pinned
//        BlockId block1 = new BlockId(testFile, 1);
//        BlockId block2 = new BlockId(testFile, 2);
//        BlockId block3 = new BlockId(testFile, 3);
//        BlockId block4 = new BlockId(testFile, 4);
//        BlockId block5 = new BlockId(testFile, 5);
//        BlockId block6 = new BlockId(testFile, 6);
//
//        // The buffer variables are meaningless. I just need the references so I can unpin them
//        // What matters is the internal bufferPool and what is mapped to what
//        Buffer buffer1 = bufferMgr.pinBuffer(block1);
//        Buffer buffer2 = bufferMgr.pinBuffer(block2);
//        Buffer buffer3 = bufferMgr.pinBuffer(block3);
//
//        // Free element 1, then 0
//        bufferMgr.unpinBuffer(buffer2);
//        bufferMgr.unpinBuffer(buffer1);
//
//        // should Pin element 0, then element 1
//        buffer1 = bufferMgr.pinBuffer(block4);
//        buffer2 = bufferMgr.pinBuffer(block5);
//
//        assertEquals(block4, bufferMgr.getBufferPool().get(0).getBlock());
//        assertEquals(block5, bufferMgr.getBufferPool().get(1).getBlock());
//
//        // unpin element 1, then 0, then 2
//        bufferMgr.unpinBuffer(buffer2);
//        bufferMgr.unpinBuffer(buffer1);
//        bufferMgr.unpinBuffer(buffer3);
//
//        // Should pin element 2, then 0, then 1
//        bufferMgr.pinBuffer(block6);
//        bufferMgr.pinBuffer(block2);
//        bufferMgr.pinBuffer(block1);
//
//        assertEquals(block6, bufferMgr.getBufferPool().get(2).getBlock());
//        assertEquals(block2, bufferMgr.getBufferPool().get(0).getBlock());
//        assertEquals(block1, bufferMgr.getBufferPool().get(1).getBlock());
//    }

    @Test
    public void testBufferLifecycle() {
        BufferMgr bufferMgr = shiDB.getBufferMgr();
        int offset = 80;

        // Same block as in the @setup function
        BlockId block = new BlockId(testFileName, 0);

        Buffer buffer1 = bufferMgr.pinBuffer(block);
        Page page1 = buffer1.getContents();

        int num = page1.getInt(offset);

        // This modification will get written to disk
        page1.setInt(offset, num + 1);
        buffer1.setModified(1, 0);
        bufferMgr.unpinBuffer(buffer1);

        // One of these pins should flush buffer1 to the disk
        Buffer buffer2 = bufferMgr.pinBuffer(new BlockId(testFileName, 2));
        bufferMgr.pinBuffer(new BlockId(testFileName, 3));
        bufferMgr.pinBuffer(new BlockId(testFileName, 4));

        bufferMgr.unpinBuffer(buffer2);

        buffer2 = bufferMgr.pinBuffer(new BlockId(testFileName, 0));
        Page page2 = buffer2.getContents();

        // Check that the value we wrote actually got flushed to disk and reloaded properly
        assertEquals(num + 1, page2.getInt(offset));

        // This won't get written to disk because the LSN is 0
        page2.setInt(offset, 42069);
        buffer2.setModified(1, 0);
        bufferMgr.unpinBuffer(buffer2);
    }
}