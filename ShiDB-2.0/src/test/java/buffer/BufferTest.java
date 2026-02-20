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
        int numTimesToPin = 69;

        assertFalse(testBuffer.isPinned());

        for (int i = 0; i < numTimesToPin; i++)
            testBuffer.pin();

        assertTrue(testBuffer.isPinned());
        assertEquals(numTimesToPin, testBuffer.getPins());
        assertNotEquals(0, testBuffer.getLastTimePinnedNano());
        assertEquals(numTimesToPin, testBuffer.getNumTimesPinned());
        assertEquals(numTimesToPin - 1, testBuffer.getMaxTimesPinnedWhileInUse());
    }

    @Test
    public void unPin() {
        int numTimesToPin = 69;
        int secondRoundPin = 420;
        int firstRoundUnpin = 42;

        for (int i = 0; i < numTimesToPin; i++)
            testBuffer.pin();

        for (int i = 0; i < firstRoundUnpin; i++)
            testBuffer.unpin();

        assertTrue(testBuffer.isPinned());

        assertEquals(firstRoundUnpin, testBuffer.getNumTimesUnpinned());

        for (int i = 0; i < numTimesToPin - firstRoundUnpin; i++)
            testBuffer.unpin();

        assertFalse(testBuffer.isPinned());
        assertEquals(0, testBuffer.getPins());
        assertNotEquals(0, testBuffer.getLastTimeUnpinnedNano());
        assertEquals(numTimesToPin, testBuffer.getNumTimesUnpinned());

        for (int i = 0; i < secondRoundPin; i++)
            testBuffer.pin();

        for (int i = 0; i < secondRoundPin; i++)
            testBuffer.unpin();

        assertEquals(secondRoundPin + numTimesToPin, testBuffer.getNumTimesUnpinned());
    }
}