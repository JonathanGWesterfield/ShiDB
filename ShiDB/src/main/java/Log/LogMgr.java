package Log;

import java.util.Iterator;

import File.Page;
import File.FileMgr;

public class LogMgr {

    public LogMgr(FileMgr fm, String logFile) {

    }

    public int append(byte[] record) {
        return 0;
    }

    public void flush(int lsn) {

    }

    public Iterator<byte[]> iterator() {
        return null;
    }
}
