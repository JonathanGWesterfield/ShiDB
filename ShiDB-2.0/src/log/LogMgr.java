package log;

import file.BlockId;
import file.FileMgr;
import file.Page;
import lombok.Getter;
import lombok.Setter;

import java.util.Iterator;

public class LogMgr {

    private FileMgr fileMgr;
    private Page logPage;
    private BlockId currentBlock;

    // The book originally had the LSN be an int, but this would be problematic in a real database due to
    // integer overflow. It may bite me in the ass later, but I'm changing this to a long to avoid that
    // Surely we can't easily overlflow a 64-bit number, right? ...... Right?
    private long latestLSN = 0; // LSN -> Log Sequence Number
    private long lastSavedLSN = 0;


    @Getter @Setter
    private String logFile;

    public LogMgr(FileMgr fileMgr, String logfile) {
        this.fileMgr = fileMgr;
        this.logFile = logfile;

        byte[] byteArr = new byte[fileMgr.getBlocksize()];
        logPage = new Page(byteArr);
        int numBlocksInLogFile = fileMgr.numBlocksInFile(logfile);
        if (numBlocksInLogFile == 0) {
            currentBlock = appendNewBlock();
        }
        else {
            currentBlock = new BlockId(logfile, numBlocksInLogFile - 1);
            fileMgr.readFromDiskToPage(currentBlock, logPage);
        }
    }

    public void flush() {
        fileMgr.writePageToDisk(currentBlock, logPage);
        lastSavedLSN = latestLSN;
    }

    public void flush(int lsn) {
        if (lsn >= lastSavedLSN)
            flush();
    }

    public Iterator<byte[]> iterator() {
        // Flush first to ensure that all logs to iterate through are on the disk
        flush();
        return new LogIterator(fileMgr, currentBlock);
    }

    public BlockId appendNewBlock() {
        BlockId block = fileMgr.append(logFile);
        logPage.setInt(0, fileMgr.getBlocksize());
        fileMgr.writePageToDisk(block, logPage);

        return block;
    }

    /**
     * Places the log records in the page from right to left instead of normal. This allows the LogIterator class
     * to read records from newest to oldest (reverse order).
     */
    public synchronized long appendRecord(byte[] logRecord) {
        // Since we read right to left in the log page, the var boundary contains the current offset location
        // we are evaluating in the record (most recently added record). We store this offset as the first 4 bytes
        // (integer size) of the page so we know where to start
        int boundary = logPage.getInt(0);
        int recordSize = logRecord.length;
        int bytesNeeded = recordSize + Integer.BYTES;

        // If the record doesn't fit, move it to a new block
        if (boundary - bytesNeeded < Integer.BYTES) {
            flush();
            currentBlock = appendNewBlock();
            boundary = logPage.getInt(0);
        }

        int recordPosition = boundary - bytesNeeded;
        logPage.setBytes(recordPosition, logRecord);
        logPage.setInt(0, recordPosition);
        latestLSN++;

        return latestLSN;
    }
}
