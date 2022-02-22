package File;

import static Constants.TestConstants.TEST_STR;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import Constants.ShiDBModules;
import Startup.ShiDB;

public class FileTest {

    public static void main(String[] args) {
        try {
            ShiDB shiDB = new ShiDB(ShiDBModules.FILE, 400).init();

            FileMgr fm = shiDB.getFileMgr();
            BlockId blk = new BlockId("testfile", 2);
            int pos1 = 88;

            Page p1 = new Page(fm.getBlockSize());
            p1.setString(pos1, TEST_STR.toString()); // write string
            int size = Page.maxLength(TEST_STR.toString().length());

            System.out.println("Test String Raw Length: " + TEST_STR.toString().length());
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

            System.out.println("Block Info: " + blk.toString());
            System.out.printf("Page 1 Offset: %d \t Value: %s\n", pos1, p1.getString(pos1));
            System.out.printf("Page 1 Offset: %d \t Value: %d\n", pos2, p1.getInt(pos2));
            System.out.printf("Page 1 Offset: %d \t Value: %d\n", pos3, p1.getShort(pos3));
            System.out.printf("Page 1 Offset: %d \t Value: %d\n", pos4, p1.getLong(pos4));
            System.out.printf("Page 1 Offset: %d \t Value: %s\n\n", pos5, p1.getDateTime(pos5).toString());

            System.out.printf("Page 2 Offset: %d \t Value: %s\n", pos1, p2.getString(pos1));
            System.out.printf("Page 2 Offset: %d \t Value: %d\n", pos2, p2.getInt(pos2));
            System.out.printf("Page 2 Offset: %d \t Value: %d\n", pos3, p2.getShort(pos3));
            System.out.printf("Page 2 Offset: %d \t Value: %d\n", pos4, p2.getLong(pos4));
            System.out.printf("Page 2 Offset: %d \t Value: %s\n", pos5, p2.getDateTime(pos5).toString());
        }
        catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }
}
