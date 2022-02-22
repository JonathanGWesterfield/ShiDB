package Log;

import static Constants.ShiDBModules.LOG;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import File.Page;
import Startup.ShiDB;

public class LogTest {

    private static LogMgr logMgr;

    public static void main(String[] args) {
        try {
            ShiDB shiDB = new ShiDB(LOG, 400).init();
            logMgr = shiDB.getLogMgr();

            clearTheFile(ShiDB.constructFileDirName(LOG), ShiDB.constructLogFileName(LOG));

            printLogRecords("The initial empty log file:");  //print an empty log file
            System.out.println();
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

    /*
    Clears the log file for testing. Otherwise, log file will throw buffer underflow exception
    since the existing file contents of the last test run will interfere with the current run.
     */
    public static void clearTheFile(String testDir, String filename) throws IOException {
        File testLogFile = new File(testDir, filename);

        if (testLogFile.exists() && testLogFile.isFile())
            testLogFile.delete();

        testLogFile.createNewFile();
    }

    private static void printLogRecords(String msg) {
        System.out.println("\n" + msg);

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
        System.out.println("\nCreating Records: ");
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
