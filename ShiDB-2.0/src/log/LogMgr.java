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
        flush();
        return new LogIterator(fileMgr, currentBlock);
    }

    public BlockId appendNewBlock() {
        BlockId block = fileMgr.append(logFile);
        logPage.setInt(0, fileMgr.getBlocksize());
        fileMgr.writePageToDisk(block, logPage);

        return block;
    }

    public int append(byte[] record) { return Integer.MAX_VALUE; }
}
