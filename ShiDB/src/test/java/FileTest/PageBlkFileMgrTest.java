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
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class FileTest {

    @Test
    public void fileTest() {
        try {
            String testFileDir = "FileTest";
            String testStr = "abcdefghijklm";

            FileMgr fm = new FileMgr(new File(testFileDir), 400);
            BlockId blk = new BlockId("testfile", 2);
            int pos1 = 88;

            Page p1 = new Page(fm.getBlockSize());
            p1.setString(pos1, testStr); // write string
            int size = Page.maxLength(testStr.length());

            System.out.printf("Test String Max Length: %d\n", size);
            int pos2 = pos1 + size;

            p1.setInt(pos2, 345); // write integer

            int pos3 = pos2 + Integer.BYTES;
            p1.setShort(pos3, (short)25); // write short

            int pos4 = pos3 + Short.BYTES;
            LocalDateTime dateTime = LocalDateTime.now();
            long epoch = dateTime.toEpochSecond(ZoneOffset.UTC);
            p1.setLong(pos4, epoch); // write long

            int pos5 = pos4 + Long.BYTES;
            p1.setDate(pos5, dateTime); // write datetime

            fm.write(blk, p1);

            Page p2 = new Page(fm.getBlockSize());
            fm.read(blk, p2);

            System.out.printf("Page 1 Offset: %d \t Value: %d\n", pos1, p1.getInt(pos1));
            System.out.printf("Page 1 Offset: %d \t Value: %s\n", pos2, p1.getString(pos2));
            System.out.printf("Page 1 Offset: %d \t Value: %d\n", pos3, p1.getShort(pos3));
            System.out.printf("Page 1 Offset: %d \t Value: %d\n", pos4, p1.getLong(pos4));
            System.out.printf("Page 1 Offset: %d \t Value: %s\n\n", pos5, p1.getDateTime(pos5).toString());

            System.out.printf("Page 2 Offset: %d \t Value: %d\n", pos1, p2.getInt(pos1));
            System.out.printf("Page 2 Offset: %d \t Value: %s\n", pos2, p2.getString(pos2));
            System.out.printf("Page 2 Offset: %d \t Value: %d\n", pos3, p2.getShort(pos3));
            System.out.printf("Page 2 Offset: %d \t Value: %d\n", pos4, p2.getLong(pos4));
            System.out.printf("Page 2 Offset: %d \t Value: %s\n", pos5, p2.getDateTime(pos5).toString());

            assertEquals(p1.getInt(pos1), p2.getInt(pos1));
            assertEquals(p1.getString(pos2), p2.getString(pos2));
            assertEquals(p1.getShort(pos3), p2.getShort(pos3));
            assertEquals(p1.getLong(pos4), p2.getLong(pos4));
            assertEquals(p1.getDateTime(pos5).toString(), p2.getDateTime(pos5).toString());
        }
        catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }
}
