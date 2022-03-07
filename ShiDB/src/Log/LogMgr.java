package Log;

import java.io.IOException;
import java.util.Iterator;

import File.BlockId;
import File.Page;
import File.FileMgr;

public class LogMgr {

    private Page logPage;
    private String logFile;
    private FileMgr fileMgr;
    private int latestLsn;
    private int lastWrittenLsn;
    private BlockId currentBlk;

    public LogMgr(FileMgr fm, String logFile) throws IOException, Exception {
        this.fileMgr = fm;
        this.logPage = new Page(fileMgr.getBlockSize());
        this.latestLsn = 0;
        this.lastWrittenLsn = 0;
        this.logFile = logFile;

        int logSize = fileMgr.eofBlockNum(logFile);

        if (logSize == 0) {
            this.currentBlk = appendNewBlock();
        }
        else {
            this.currentBlk = new BlockId(logFile, logSize - 1);
            fm.read(currentBlk, logPage);
        }
    }

    /**
     * To the user, the record is written to the disk in the log file. What really happens is
     * this function writes records into the log page. If the record doesn't fit into the page,
     * write the page to disk and overwrite the page. LogRecord are written right to left.
     * This allows the iterator to read left to write (newest to oldest record).
     * @param logRecord The record that needs to get written to the disk
     * @return Latest Log Sequencing Number (LSN).
     */
    public synchronized int append(byte[] logRecord) throws Exception {
        int pageBoundary = logPage.getInt(0); // offset of most recently added record
        int recordSize = logRecord.length;
        int bytesNeeded = recordSize + Integer.BYTES; // need int to specify size of record in the page

        // calculate if record can fit into page, if not write page to disk and clear it
        if(pageBoundary - bytesNeeded < Integer.BYTES) { // too big. Need to write page to disk
            flush();
            currentBlk = appendNewBlock();
            pageBoundary = logPage.getInt(0); // page offset will change
        }

        int recordPos = pageBoundary - bytesNeeded;
        logPage.setBytes(recordPos, logRecord);
        logPage.setInt(0, recordPos); // sets the new space left over in the page
        ++latestLsn;
        return latestLsn;
    }

    public void flush(int lsn) {
        if (lsn >= lastWrittenLsn)
            flush();
    }

    public void flush() {
        fileMgr.write(currentBlk, logPage);
        lastWrittenLsn = latestLsn;
//        logPage.clear();
    }

    /**
     * Appends empty block to the end of the log file by creating an empty page and
     * writing it to the disk. We store the size of the page in the first int in the
     * page. This int is important for the append method to calculate page size.
     * @return The blockId of the new empty block at the end of the log file.
     */
    private BlockId appendNewBlock() throws Exception {
        BlockId blk = fileMgr.append(logFile);
        logPage.setInt(0, fileMgr.getBlockSize());
        fileMgr.write(blk, logPage); // write empty log page to disk
        return blk;
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fileMgr, currentBlk);
    }
}
