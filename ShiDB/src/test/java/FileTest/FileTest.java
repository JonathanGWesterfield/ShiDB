package FileTest;


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
    public static void fileTest() {
        try {
            String testFilename = "testDB.shidb";
            String testString = "abcdefghijklm";
            File testFile = new File(testFilename);

            RandomAccessFile file = new RandomAccessFile(testFile, "rw");

            FileMgr fileManager = new FileMgr(testFilename, 400);

            BlockId blk = new BlockId(testFilename, 2);

            Page page1 = new Page(fileManager.blockSize());

            int pos1 = 88;

            page1.setString(pos1, testFilename);

            int size = Page.maxLength(testFilename.length());
            int pos2 = pos1 + size;

            page1.setInt(pos2,345);

            Page page2 = new Page(fileManager.blockSize());

            fileManager.read(blk, page2);
        }
        catch (java.io.FileNotFoundException e) {
            System.out.println(e);
        }
    }
}
