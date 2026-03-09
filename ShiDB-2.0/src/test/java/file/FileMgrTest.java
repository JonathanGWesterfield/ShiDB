package file;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import server.ShiDB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This isn't a technically a unit test per se because we are testing both the FileMgr
 * and the underlying Page class as well.
 */

class FileMgrTest {
    private final String TEST_DIR = "FileMgr-Unit-test";

    private ShiDB shiDB;
    private FileMgr fileMgr;

    @BeforeEach
    void setUp() throws IOException {
        shiDB = new ShiDB(TEST_DIR, 600);
        fileMgr = shiDB.getFileMgr();
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
    @DisplayName("Write an integer and string to 1 page, write to disk, then read to new page")
    public void testFileMgr() {
        BlockId blk = new BlockId("testfile", 2);
        Page page1 = new Page(fileMgr.getBlocksize());

        int position1 = 88;
        String writeString = "The database name is a pun, isn't it?";

        page1.setString(position1, writeString);
        int byteLength = Page.calcMaxByteLength(writeString);

        int position2 = position1 + byteLength;
        page1.setInt(position2, 345);

        fileMgr.writePageToDisk(blk, page1);
        Page page2 = new Page(fileMgr.getBlocksize());
        fileMgr.readFromDiskToPage(blk, page2);

        assertEquals(page1.getString(position1), page2.getString(position1));
        assertEquals(page1.getInt(position2), page2.getInt(position2));
    }


    @Test
    @DisplayName("Check the number of writes to disk are being tracked correctly")
    public void testFileWritesBeingTracked() {
        // The startup of other portions of the database (logMgr, bufferMgr, etc) also perform reads and writes
        // To avoid those breaking this unit test, we will reset the counter to 0
        fileMgr.resetFileMgrStatistics();

        BlockId blk = new BlockId("testfile", 2);
        Page page1 = new Page(fileMgr.getBlocksize());

        int position1 = 88;
        String writeString = "The database name is a pun, isn't it?";

        page1.setString(position1, writeString);

        int numExpectedWrites = 4;
        for (int i = 0; i < numExpectedWrites; i++)
            fileMgr.writePageToDisk(blk, page1);

        assertEquals(numExpectedWrites, (int) fileMgr.getBlocksWriteCounter());
    }

    @Test
    @DisplayName("Check the number of reads from disk are being tracked correctly")
    public void testFileReadsBeingTracked() {
        // The startup of other portions of the database (logMgr, bufferMgr, etc) also perform reads and writes
        // To avoid those breaking this unit test, we will reset the counter to 0
        fileMgr.resetFileMgrStatistics();

        BlockId blk = new BlockId("testfile", 2);
        Page page1 = new Page(fileMgr.getBlocksize());

        int position1 = 88;
        String writeString = "The database name is a pun, isn't it?";

        page1.setString(position1, writeString);

        // Write to disk so we can read it
        fileMgr.writePageToDisk(blk, page1);

        int numExpectedReads = 4;
        for (int i = 0; i < numExpectedReads; i++)
            fileMgr.readFromDiskToPage(blk, page1);

        assertEquals(numExpectedReads, (int) fileMgr.getBlocksReadCounter());
    }

    @Test
    @DisplayName("Check that number of times various files are appended to")
    public void checkNumFileAppends() {
        String filename1 = "test1";
        String fileName2 = "test2";
        String fileName3 = "test3";

        Map<String, Integer> numExpectedAppends = new HashMap<>();

        // Setup how many times we want to append these files
        // Want a different num of appends for each one to make obvious that this works or not
        numExpectedAppends.put(filename1, 3);
        numExpectedAppends.put(fileName2, 9);
        numExpectedAppends.put(fileName3, 1);

        // Setup the appends and check
        for (Map.Entry<String, Integer> entry : numExpectedAppends.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                fileMgr.append(entry.getKey());
            }

            assertEquals(entry.getValue(), fileMgr.getNumAppends(entry.getKey()));
        }

        // Cleanup
        for (Map.Entry<String, Integer> entry: numExpectedAppends.entrySet())
            fileMgr.deleteFile(entry.getKey());
    }

    // This unnecessarily tests out the fileMgr read/write in addition to the intended test. I don't care.
    // I'm just copy pasting this for speed. If this breaks because of the fileMgr, I will accept my fate
    @Test
    @DisplayName("Test out other setters/getters in the Page class byte/boolean/short/date/long/double, etc")
    public void testOtherDateTypeSettersAndGetters() {
        BlockId blk = new BlockId("testfile", 2);

        Page page1 = new Page(fileMgr.getBlocksize());

        int position1 = 88;

        // Test a short first
        page1.setShort(position1, (short)25); // write short

        int position2 = position1 + Short.BYTES;

        // Test and epoch (long)
        LocalDateTime dateTime = LocalDateTime.now();
        long epoch = dateTime.toEpochSecond(ZoneOffset.UTC);
        page1.setLong(position2, epoch); // write long

        // Test epoch again, but the abstraction this time
        int position3 = position2 + Long.BYTES;
        page1.setDateTime(position3, dateTime); // write datetime

        // Test out a double
        int position4 = position3 + Double.BYTES;
        page1.setDouble(position4, 3.141593);

        fileMgr.writePageToDisk(blk, page1);

        Page page2 = new Page(fileMgr.getBlocksize());
        fileMgr.readFromDiskToPage(blk, page2);

        assertEquals(page1.getShort(position1), page2.getShort(position1));
        assertEquals(page1.getLong(position2), page2.getLong(position2));
        assertEquals(page1.getDateTime(position3), page2.getDateTime(position3));
        assertEquals(page1.getDouble(position4), page2.getDouble(position4));
    }

    /**
     * Directly tests the getBytes/setBytes round trip since this is where the absolute vs relative
     * ByteBuffer read bug manifests. The bug causes getBytes() to return data shifted 4 bytes back,
     * overlapping with the length prefix.
     */
    @Test
    @DisplayName("getBytes returns exact bytes that were written by setBytes")
    public void testGetBytesRoundTrip() {
        Page page = new Page(fileMgr.getBlocksize());
        byte[] original = new byte[]{10, 20, 30, 40, 50};
        int offset = 0;

        page.setBytes(offset, original);
        byte[] retrieved = page.getBytes(offset);

        assertArrayEquals(original, retrieved, "getBytes should return the exact bytes written by setBytes");
    }

    /**
     * Verifies getBytes works correctly at a non-zero offset, ruling out any accidental
     * offset-zero special case that could mask the bug.
     */
    @Test
    @DisplayName("getBytes returns correct bytes at non-zero offset")
    public void testGetBytesAtOffset() {
        Page page = new Page(fileMgr.getBlocksize());
        byte[] original = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        int offset = 20;

        page.setBytes(offset, original);
        byte[] retrieved = page.getBytes(offset);

        assertArrayEquals(original, retrieved, "getBytes should return correct bytes at non-zero offset");
    }

    /**
     * Verifies getString round trip since getString delegates to getBytes.
     * A bug in getBytes would corrupt string reads.
     */
    @Test
    @DisplayName("getString returns exact string written by setString")
    public void testGetStringRoundTrip() {
        Page page = new Page(fileMgr.getBlocksize());
        String original = "ShiDB is a pun";
        int offset = 0;

        page.setString(offset, original);
        String retrieved = page.getString(offset);

        assertEquals(original, retrieved, "getString should return the exact string written by setString");
    }

    /**
     * Verifies two values written adjacent to each other don't bleed into each other.
     * This catches off-by-one errors in position advancement after reads.
     */
    @Test
    @DisplayName("Adjacent byte arrays are read back independently without corruption")
    public void testAdjacentBytesRoundTrip() {
        Page page = new Page(fileMgr.getBlocksize());
        byte[] first = new byte[]{1, 2, 3};
        byte[] second = new byte[]{4, 5, 6, 7, 8};

        int firstOffset = 0;
        int secondOffset = Integer.BYTES + first.length; // size of [length prefix + data]

        page.setBytes(firstOffset, first);
        page.setBytes(secondOffset, second);

        assertArrayEquals(first, page.getBytes(firstOffset), "First byte array should be read back correctly");
        assertArrayEquals(second, page.getBytes(secondOffset), "Second byte array should be read back correctly without bleeding from first");
    }

    /**
     * Verifies that writing to disk and reading back preserves byte array contents.
     * Catches any serialization issues on top of the in-memory correctness.
     */
    @Test
    @DisplayName("getBytes survives a write to disk and read back")
    public void testGetBytesSurvivesDiskRoundTrip() {
        BlockId blk = new BlockId("testfile", 99);
        Page writePage = new Page(fileMgr.getBlocksize());
        byte[] original = new byte[]{9, 8, 7, 6, 5, 4, 3, 2, 1};
        int offset = 0;

        writePage.setBytes(offset, original);
        fileMgr.writePageToDisk(blk, writePage);

        Page readPage = new Page(fileMgr.getBlocksize());
        fileMgr.readFromDiskToPage(blk, readPage);

        assertArrayEquals(original, readPage.getBytes(offset), "getBytes should return correct data after disk round trip");
    }
    
}