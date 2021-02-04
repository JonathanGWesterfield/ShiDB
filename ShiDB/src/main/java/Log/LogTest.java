package Log;

import java.io.File;
import java.util.Iterator;

import File.FileMgr;
import File.Page;

public class LogTest {

    private static LogMgr logMgr;

    public static void main(String[] args) {
        try {
            String testFileDir = "FileTest";
            String logFilename = "testLogFile.shidb.log";

            FileMgr fm = new FileMgr(new File(testFileDir), 400);

            logMgr = new LogMgr(fm, logFilename);
            createRecords(1, 35);
            printLogRecords("Log file has been populated with the following records: ");
            createRecords(36, 70);

            logMgr.flush(65);
            printLogRecords("Log file has been populated with the following records: ");
        }
        catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }

    private static void printLogRecords(String msg) {
        System.out.println(msg);

        Iterator<byte[]> iter = logMgr.iterator();

        while (iter.hasNext()) {
            byte[] record = iter.next();
            Page page = new Page(record);
            String str = page.getString(0);
            int nextPos = Page.maxLength(str.length());
            int val = page.getInt(nextPos);

            System.out.printf("[%s, %d]\n", str, val);
        }
    }

    private static void createRecords(int start, int end) throws Exception {
        System.out.println("Creating Records: ");
        String recordStr = null;
        int recordNum = 0;

        for (int i = start; i <= end; ++i) {
            recordStr = "record-" + i;
            byte[] record = createLogRecord(recordStr, i+100);
            int lsn = logMgr.append(record);
            System.out.print(lsn + " ");
        }
        System.out.println();
    }

    private static byte[] createLogRecord(String s, int n) throws Exception {
        int nextPos = Page.maxLength(s.length());
        byte[] arr = new byte[nextPos + Integer.BYTES];
        Page page = new Page(arr);
        page.setString(0, s);
        page.setInt(nextPos, n);
        return arr;
    }
}
