package buffer;

import file.BlockId;
import file.Page;
import file.FileMgr;
import log.LogMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import server.ConfigFetcher;
import server.ShiDB;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class BufferTest {

    @Mock
    FileMgr fileMgr;

    @Mock
    LogMgr logMgr;

    Buffer testBuffer;

    private ShiDB shiDB;

    private final String testFileName = "bufferTestFile";

    @BeforeEach
    void setUp() throws IOException {
        this.shiDB = new ShiDB("Buffer-unit-test", ConfigFetcher.getDBFileBlockSize(), ConfigFetcher.getSizeOfBufferPool());

        this.fileMgr = shiDB.getFileMgr();
        this.logMgr = shiDB.getLogMgr();

        this.testBuffer = new Buffer(this.fileMgr, this.logMgr);

        BlockId block = new BlockId(testFileName, 0);
        Page page = new Page(fileMgr.getBlocksize());

        int position1 = 80;
        page.setInt(position1, 345);
        fileMgr.writePageToDisk(block, page);
    }

    @Test
    public void testPin() {
        assertFalse(testBuffer.isPinned());
        testBuffer.pin();
        testBuffer.pin();
        testBuffer.pin();
        assertTrue(testBuffer.isPinned());
        assertEquals(3, testBuffer.getPins());
        assertNotEquals(0, testBuffer.getLastTimePinned());
    }

    @Test
    public void unPin() {
        testBuffer.pin();
        testBuffer.pin();
        assertTrue(testBuffer.isPinned());

        testBuffer.unpin();
        assertTrue(testBuffer.isPinned());

        testBuffer.unpin();
        assertFalse(testBuffer.isPinned());
        assertEquals(0, testBuffer.getPins());
        assertNotEquals(0, testBuffer.getLastTimeUnpinned());
    }
}