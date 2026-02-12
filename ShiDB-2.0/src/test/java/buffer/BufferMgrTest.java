package buffer;

import error.BufferAbortException;
import file.BlockId;
import org.junit.jupiter.api.*;
import server.ConfigFetcher;
import server.ShiDB;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static server.ConfigFetcher.getConfigs;

public class BufferMgrTest {
    private ShiDB shiDB;
    private BufferMgr bufferMgr;
    private int numInitialBuffers;
    private long bufferMgrUnitTestWaitTime;

    @BeforeEach
    void setUp() throws IOException {
        // This config shortens the wait timeout for buffer pinning to shorten time for unit testing
        getConfigs("src/test/resources/bufferMgrTestConfig.json");
        bufferMgrUnitTestWaitTime = ConfigFetcher.getBufferMgrMaxWaitTime();
        numInitialBuffers = ConfigFetcher.getSizeOfBufferPool();
        this.shiDB = new ShiDB("BufferMgr-unit-test", ConfigFetcher.getDBFileBlockSize(), numInitialBuffers);
        bufferMgr = shiDB.getBufferMgr();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    @DisplayName("Test the pin function")
    public void testPin() {
        Buffer testBuffer = bufferMgr.pinBuffer(new BlockId("testfile", 0));

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
        bufferMgr = new BufferMgr(shiDB.getFileMgr(), shiDB.getLogMgr(), numInitialBuffers);

        Buffer testBuffer2 = bufferMgr.pinBuffer(new BlockId("testfile", 0));
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
        // Then we ensure that the buffer pool layout looks like we expect it to
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
}