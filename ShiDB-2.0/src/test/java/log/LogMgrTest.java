package log;

import file.FileMgr;
import file.Page;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import server.ShiDB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

class LogMgrTest {
    private final String TEST_DIR = "Log-unit-test";

    private FileMgr fileMgr;
    private ShiDB shiDB;
    private LogMgr logMgr;

    // Since log records are created backwards ( [value][header] -> [value][header]), we need to manually create
    // these fake log records backwards, hence why we set the string first, then set the number
    private byte[] createLogRecord(String str, int num) {
        int numPosition = Page.calcMaxByteLength(str);
        byte[] byteArr = new byte[numPosition + Integer.BYTES];
        Page page = new Page(byteArr);

        page.setString(0, str);
        page.setInt(numPosition, num);

        return byteArr;
    }

    private void createRecords(int start, int end) {
        System.out.print("LSN's: ");
        for (int i = start; i < end; i++) {
            byte[] record = createLogRecord("record" + i, i + 100);
            long lsn = logMgr.appendRecord(record);
            System.out.print(lsn + " ");
        }
        System.out.println();
    }

    public static void clearFile(String testDir, String filename) throws IOException {
        File testLogFile = new File(testDir, filename);

        if (testLogFile.exists() && testLogFile.isFile())
            testLogFile.delete();

        testLogFile.createNewFile();
    }

    @BeforeEach
    void setUp() throws IOException {
        shiDB = new ShiDB(TEST_DIR, 600);
        fileMgr = shiDB.getFileMgr();
        logMgr = shiDB.getLogMgr();

        // Since only a single log file is used for the whole log manager, need to clear the file each time so
        // each unit test runs properly
        clearFile(TEST_DIR, logMgr.getLogFile());
    }

    @AfterEach
    void cleanUp() throws IOException {
        deleteDirectory(TEST_DIR);
    }

    private void deleteDirectory(String dirName) throws IOException {
        Path path = Path.of(dirName);
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Test log appends")
    public void testLogAppend() {
        int numExpectedRecords = 30;
        // Use the log iterator to count how many records were appended
        ArrayList<byte[]> records = new ArrayList<>();

        createRecords(1, numExpectedRecords + 1);

        Iterator<byte[]> logIterator = logMgr.iterator();
        while(logIterator.hasNext()) {
            records.add(logIterator.next());
        }

        assertEquals(numExpectedRecords, records.size());
    }
    
    @Test
    @DisplayName("Test log flush")
    public void testFlush() {
        int round1 = 30;
        int round2 = 60;
        int round3 = 7;
        int numExpectedRecords = round1 + round2 + round3;

        // Use the log iterator to count how many records were appended
        ArrayList<byte[]> records = new ArrayList<>();

        createRecords(1, round1 + 1);

        createRecords(round1 + 1, round1 + round2 + 1);

        createRecords(round2 + 1, round2 + round3 + 1);

        // The call to iterator implicitly calls flush()
        Iterator<byte[]> logIterator = logMgr.iterator();
        while(logIterator.hasNext()) {
            records.add(logIterator.next());
        }

        assertEquals(numExpectedRecords, records.size());
    }

    @Test
    @DisplayName("Test log iterator returns correct record contents")
    public void testLogIteratorContents() throws IOException {
        // Write 3 known records
        byte[] record1 = createLogRecord("alpha", 1);
        byte[] record2 = createLogRecord("beta", 2);
        byte[] record3 = createLogRecord("gamma", 3);

        logMgr.appendRecord(record1);
        logMgr.appendRecord(record2);
        logMgr.appendRecord(record3);

        // Iterator returns newest to oldest
        Iterator<byte[]> iter = logMgr.iterator();

        byte[] returned3 = iter.next();
        byte[] returned2 = iter.next();
        byte[] returned1 = iter.next();

        assertArrayEquals(record3, returned3, "Third record should be returned first (newest)");
        assertArrayEquals(record2, returned2, "Second record should be returned second");
        assertArrayEquals(record1, returned1, "First record should be returned last (oldest)");
        assertFalse(iter.hasNext(), "Iterator should be exhausted after 3 records");
    }

    /**
     * Verifies that the LogIterator correctly crosses block boundaries.
     *
     * Block size: 600 bytes (based on the setup function())
     * Record layout: [length prefix (4)] + [string (4 + 7)] + [int (4)] = 19 bytes per record
     * Usable space per block: 600 - 4 (boundary pointer) = 596 bytes
     * Records per block: floor(596 / 19) = 31
     *
     * Writing 40 records forces a second block (31 in block 0, 9 in block 1).
     * The iterator must cross from block 1 back to block 0 and return all records
     * in correct newest-to-oldest order with correct contents.
     */
    @Test
    @DisplayName("Log iterator correctly crosses block boundaries")
    public void testLogIteratorCrossesBlockBoundary() {
        int numRecords = 40; // deliberately more than 31 to force a second block
        ArrayList<byte[]> writtenRecords = new ArrayList<>();

        for (int i = 1; i <= numRecords; i++) {
            byte[] record = createLogRecord("record" + i, i + 100);
            writtenRecords.add(record);
            logMgr.appendRecord(record);
        }

        // Collect all records back via the iterator (newest to oldest)
        ArrayList<byte[]> readRecords = new ArrayList<>();
        Iterator<byte[]> iter = logMgr.iterator();
        while (iter.hasNext())
            readRecords.add(iter.next());

        // Verify count — no records lost crossing the boundary
        assertEquals(numRecords, readRecords.size(),
                "All records should be returned across block boundaries");

        // Verify order and contents — iterator returns newest to oldest
        // so readRecords[0] == writtenRecords[39], readRecords[39] == writtenRecords[0]
        for (int i = 0; i < numRecords; i++) {
            int writtenIndex = numRecords - 1 - i;
            assertArrayEquals(writtenRecords.get(writtenIndex), readRecords.get(i),
                    String.format("Record at iterator position %d should match written record %d", i, writtenIndex));
        }

        // Spot-check the boundary records explicitly —
        // record 31 is the last record written to block 0 (oldest in block 0)
        // record 32 is the first record written to block 1 (newest in block 1, read first)
        // In read order (newest-to-oldest): record40, record39, ... record32 are in block 1,
        // record31, record30, ... record1 are in block 0
        byte[] expectedFirstInBlock1 = writtenRecords.get(numRecords - 1); // record40
        byte[] expectedLastInBlock1  = writtenRecords.get(30);             // record31 (0-indexed: index 30 = record31... wait, this is the boundary)

        // The first 9 read back (indices 0-8) should be from block 1 (records 40 down to 32)
        // The next 31 read back (indices 9-39) should be from block 0 (records 31 down to 1)
        assertArrayEquals(writtenRecords.get(39), readRecords.get(0),
                "First record read should be record40 (newest, in block 1)");
        assertArrayEquals(writtenRecords.get(31), readRecords.get(8),
                "9th record read should be record32 (oldest in block 1, just before boundary)");
        assertArrayEquals(writtenRecords.get(30), readRecords.get(9),
                "10th record read should be record31 (newest in block 0, just after boundary crossing)");
        assertArrayEquals(writtenRecords.get(0), readRecords.get(39),
                "Last record read should be record1 (oldest, in block 0)");
    }

    @Test
    @DisplayName("Log iterator hasNext returns false after crossing boundary and exhausting all records")
    public void testLogIteratorExhaustsAfterBoundaryCrossing() {
        int numRecords = 40;

        for (int i = 1; i <= numRecords; i++)
            logMgr.appendRecord(createLogRecord("record" + i, i));

        Iterator<byte[]> iter = logMgr.iterator();
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }

        assertEquals(numRecords, count, "Iterator should return exactly numRecords before exhausting");
        assertFalse(iter.hasNext(), "Iterator should report hasNext=false after all records are consumed");
    }
}