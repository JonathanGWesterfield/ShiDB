package FileTest;


import static org.junit.Assert.assertEquals;

import File.FileMgr;
import File.BlockId;
import File.Page;

import org.junit.Test;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;

public class FileTest {

    @Test
    public void fileTest() {
        try {
            String testFileDir = "FileTest";
            String testString = "abcdefghijklm";

            FileMgr fileManager = new FileMgr(new File(testFileDir), 400);

            BlockId blk = new BlockId(testFileDir, 2);

            Page page1 = new Page(fileManager.blockSize());

            int pos1 = 88;

            page1.setString(pos1, testFileDir);

            int size = Page.maxLength(testFileDir.length());
            int pos2 = pos1 + size;

            page1.setInt(pos2,345);

            Page page2 = new Page(fileManager.blockSize());

            fileManager.read(blk, page2);

            assertEquals(page1.getInt(88), testString);
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
}
