package File;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import Server.ShiDB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * This isn't technically a unit test per se because we are testing both the FileMgr
 * and the underlying Page class as well.
 */
class FileMgrTest {
    private final String TEST_DIR = "FileMgr-Unit-test";

    private ShiDB shiDB;
    private FileMgr fileMgr;

    @BeforeEach
    void setUp() throws IOException {
        deleteDirectory(TEST_DIR);  // wipe stale state from any previous crashed run
        shiDB = new ShiDB(TEST_DIR, 600);
        fileMgr = shiDB.getFileMgr();
        fileMgr.resetFileMgrStatistics();
    }

    @AfterEach
    void cleanUp() throws IOException {
        deleteDirectory(TEST_DIR);
    }

    private void deleteDirectory(String dirName) throws IOException {
        Path path = Path.of(dirName);
        if (Files.exists(path))
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
    }

    @Test
    @DisplayName("Write an integer and string to 1 page, write to disk, then read to new page")
    public void testFileMgr() {
        BlockId blk = fileMgr.append("testfile");
        Page page1 = new Page(fileMgr.getBlocksize());

        int position1 = 88;
        String writeString = "The database name is a pun, isn't it?";
        page1.setString(position1, writeString);

        int position2 = position1 + Page.calcMaxByteLength(writeString);
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
        BlockId blk = fileMgr.append("testfile");
        Page page1 = new Page(fileMgr.getBlocksize());
        page1.setString(88, "The database name is a pun, isn't it?");

        int numExpectedWrites = 4;
        for (int i = 0; i < numExpectedWrites; i++)
            fileMgr.writePageToDisk(blk, page1);

        assertEquals(numExpectedWrites, (int) fileMgr.getBlocksWriteCounter());
    }

    @Test
    @DisplayName("Check the number of reads from disk are being tracked correctly")
    public void testFileReadsBeingTracked() {
        BlockId blk = fileMgr.append("testfile");
        Page page1 = new Page(fileMgr.getBlocksize());
        page1.setString(88, "The database name is a pun, isn't it?");
        fileMgr.writePageToDisk(blk, page1);
        fileMgr.resetFileMgrStatistics(); // exclude the setup write from the read count

        int numExpectedReads = 4;
        for (int i = 0; i < numExpectedReads; i++)
            fileMgr.readFromDiskToPage(blk, page1);

        assertEquals(numExpectedReads, (int) fileMgr.getBlocksReadCounter());
    }

    @Test
    @DisplayName("Append counter tracks independently per file")
    public void checkNumFileAppends() {
        Map<String, Integer> numExpectedAppends = new HashMap<>();
        numExpectedAppends.put("test1", 3);
        numExpectedAppends.put("test2", 9);
        numExpectedAppends.put("test3", 1);

        for (Map.Entry<String, Integer> entry : numExpectedAppends.entrySet())
            for (int i = 0; i < entry.getValue(); i++)
                fileMgr.append(entry.getKey());

        // @AfterEach handles cleanup — no inline cleanup needed here
        for (Map.Entry<String, Integer> entry : numExpectedAppends.entrySet())
            assertEquals(entry.getValue(), fileMgr.getNumAppends(entry.getKey()),
                    "Append count mismatch for: " + entry.getKey());
    }

    @Test
    @DisplayName("Test out other setters/getters in the Page class: short, long, DateTime, double")
    public void testOtherDateTypeSettersAndGetters() {
        BlockId blk = fileMgr.append("testfile");
        Page page1 = new Page(fileMgr.getBlocksize());

        int position1 = 88;
        page1.setShort(position1, (short) 25);

        int position2 = position1 + Short.BYTES;
        LocalDateTime dateTime = LocalDateTime.now();
        long epoch = dateTime.toEpochSecond(ZoneOffset.UTC);
        page1.setLong(position2, epoch);

        int position3 = position2 + Long.BYTES;
        page1.setDateTime(position3, dateTime);

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

        page.setBytes(0, original);
        byte[] retrieved = page.getBytes(0);

        assertArrayEquals(original, retrieved,
                "getBytes should return the exact bytes written by setBytes");
    }

    @Test
    @DisplayName("getBytes returns correct bytes at non-zero offset")
    public void testGetBytesAtOffset() {
        Page page = new Page(fileMgr.getBlocksize());
        byte[] original = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        int offset = 20;

        page.setBytes(offset, original);
        byte[] retrieved = page.getBytes(offset);

        assertArrayEquals(original, retrieved,
                "getBytes should return correct bytes at non-zero offset");
    }

    @Test
    @DisplayName("getString returns exact string written by setString")
    public void testGetStringRoundTrip() {
        Page page = new Page(fileMgr.getBlocksize());
        String original = "ShiDB is a pun";

        page.setString(0, original);

        assertEquals(original, page.getString(0),
                "getString should return the exact string written by setString");
    }

    @Test
    @DisplayName("Adjacent byte arrays are read back independently without corruption")
    public void testAdjacentBytesRoundTrip() {
        Page page = new Page(fileMgr.getBlocksize());
        byte[] first = new byte[]{1, 2, 3};
        byte[] second = new byte[]{4, 5, 6, 7, 8};
        int firstOffset = 0;
        int secondOffset = Integer.BYTES + first.length;

        page.setBytes(firstOffset, first);
        page.setBytes(secondOffset, second);

        assertArrayEquals(first, page.getBytes(firstOffset),
                "First byte array should be read back correctly");
        assertArrayEquals(second, page.getBytes(secondOffset),
                "Second byte array should not be corrupted by the first");
    }

    @Test
    @DisplayName("getBytes survives a write to disk and read back")
    public void testGetBytesSurvivesDiskRoundTrip() {
        BlockId blk = fileMgr.append("testfile");
        Page writePage = new Page(fileMgr.getBlocksize());
        byte[] original = new byte[]{9, 8, 7, 6, 5, 4, 3, 2, 1};

        writePage.setBytes(0, original);
        fileMgr.writePageToDisk(blk, writePage);

        Page readPage = new Page(fileMgr.getBlocksize());
        fileMgr.readFromDiskToPage(blk, readPage);

        assertArrayEquals(original, readPage.getBytes(0),
                "getBytes should return correct data after disk round trip");
    }

    @Test
    @DisplayName("deleteFile removes the file from disk")
    public void testDeleteFileRemovesFileFromDisk() {
        fileMgr.append("todelete");
        fileMgr.deleteFile("todelete");

        assertFalse(new File(TEST_DIR, "todelete").exists(),
                "File must not exist on disk after deleteFile()");
    }

    @Test
    @DisplayName("deleteFile removes the handle from openFiles so the file can be reopened cleanly")
    public void testDeleteFileRemovesHandleFromOpenFiles() {
        fileMgr.append("todelete");
        fileMgr.deleteFile("todelete");

        // If the stale closed handle remained in openFiles, append would try to
        // use it and throw an IOException
        assertDoesNotThrow(() -> fileMgr.append("todelete"),
                "Should be able to reopen a file after deleteFile() without a stale handle error");
    }

    @Test
    @DisplayName("deleteFile resets the append counter for that file")
    public void testDeleteFileResetsAppendCounter() {
        fileMgr.append("todelete");
        fileMgr.append("todelete");
        fileMgr.deleteFile("todelete");

        // After deletion the file no longer exists — appending should start fresh at 1
        fileMgr.append("todelete");
        assertEquals(1, fileMgr.getNumAppends("todelete"),
                "Append counter should reset after file is deleted and recreated");
    }

    @Test
    @DisplayName("setDateTime and getDateTime round-trip correctly — catches timezone conversion bug")
    public void testDateTimeRoundTrip() {
        BlockId blk = fileMgr.append("testfile");
        Page writePage = new Page(fileMgr.getBlocksize());
        LocalDateTime original = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

        writePage.setDateTime(0, original);

        assertEquals(original, writePage.getDateTime(0),
                "getDateTime should return exact value set by setDateTime — catches UTC vs system timezone mismatch");

        fileMgr.writePageToDisk(blk, writePage);
        Page readPage = new Page(fileMgr.getBlocksize());
        fileMgr.readFromDiskToPage(blk, readPage);

        assertEquals(original, readPage.getDateTime(0),
                "getDateTime should survive a disk round-trip without timezone drift");
    }
}