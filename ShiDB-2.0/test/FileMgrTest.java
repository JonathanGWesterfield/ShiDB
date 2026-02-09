import file.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import server.ShiDB;
import file.FileMgr;
import file.BlockId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileMgrTest {
    private ShiDB shiDB;
    private FileMgr fileMgr;

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws IOException {
        shiDB = new ShiDB("FileMgr-Unit-test", 600);
        fileMgr = shiDB.getFileMgr();
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
        BlockId blk = new BlockId("testfile", 2);
        Page page1 = new Page(fileMgr.getBlocksize());

        int position1 = 88;
        String writeString = "The database name is a pun, isn't it?";

        page1.setString(position1, writeString);

        int numExpectedWrites = 4;
        for (int i = 0; i < numExpectedWrites; i++)
            fileMgr.writePageToDisk(blk, page1);

        assertEquals(numExpectedWrites, fileMgr.getNumBlocksWritten());
    }

    @Test
    @DisplayName("Check the number of reads from disk are being tracked correctly")
    public void testFileReadsBeingTracked() {
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

        assertEquals(numExpectedReads, fileMgr.getNumBlocksRead());
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

}