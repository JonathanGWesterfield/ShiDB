package log;

import file.FileMgr;
import file.Page;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import server.ShiDB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class LogMgrTest {

    private FileMgr fileMgr;
    private ShiDB shiDB;
    private LogMgr logMgr;

    // Since log records are created backwards ( [value][header] -> [value][header]), we need to manually create
    // these fake log records backwards, hence why we set the string first, then set the number
    private byte[] createLogRecord(String str, int num) {
        int numPosition = Page.calcMaxByteLength(str);
        byte[] byteArr = new byte[numPosition + Integer.BYTES];
        Page page = new Page(byteArr);

        page.setString(0, str);
        page.setInt(numPosition, num);

        return byteArr;
    }

    private void createRecords(int start, int end) {
        System.out.print("LSN's: ");
        for (int i = start; i < end; i++) {
            byte[] record = createLogRecord("record" + i, i + 100);
            long lsn = logMgr.appendRecord(record);
            System.out.print(lsn + " ");
        }
        System.out.println();
    }

    public static void clearFile(String testDir, String filename) throws IOException {
        File testLogFile = new File(testDir, filename);

        if (testLogFile.exists() && testLogFile.isFile())
            testLogFile.delete();

        testLogFile.createNewFile();
    }

    @BeforeEach
    void setUp() throws IOException {
        String testDir = "Log-unit-test";
        shiDB = new ShiDB(testDir, 600);
        fileMgr = shiDB.getFileMgr();
        logMgr = shiDB.getLogMgr();

        // Since only a single log file is used for the whole log manager, need to clear the file each time so
        // each unit test runs properly
        clearFile(testDir, logMgr.getLogFile());
    }

    @Test
    @DisplayName("Test log appends")
    public void testLogAppend() {
        int numExpectedRecords = 30;
        // Use the log iterator to count how many records were appended
        ArrayList<byte[]> records = new ArrayList<>();

        createRecords(1, numExpectedRecords + 1);

        Iterator<byte[]> logIterator = logMgr.iterator();
        while(logIterator.hasNext()) {
            records.add(logIterator.next());
        }

        assertEquals(numExpectedRecords, records.size());
    }
    
    @Test
    @DisplayName("Test log flush")
    public void testFlush() {
        int round1 = 30;
        int round2 = 60;
        int round3 = 7;
        int numExpectedRecords = round1 + round2 + round3;

        // Use the log iterator to count how many records were appended
        ArrayList<byte[]> records = new ArrayList<>();

        createRecords(1, round1 + 1);

        createRecords(round1 + 1, round1 + round2 + 1);

        createRecords(round2 + 1, round2 + round3 + 1);

        // The call to iterator implicitly calls flush()
        Iterator<byte[]> logIterator = logMgr.iterator();
        while(logIterator.hasNext()) {
            records.add(logIterator.next());
        }

        assertEquals(numExpectedRecords, records.size());
    }
}