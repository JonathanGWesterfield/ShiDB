import file.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import server.ShiDB;
import file.FileMgr;
import file.BlockId;

import java.io.IOException;

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
}


