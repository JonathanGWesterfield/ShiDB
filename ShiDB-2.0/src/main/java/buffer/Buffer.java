package buffer;

import file.BlockId;
import file.FileMgr;
import file.Page;
import log.LogMgr;
import lombok.Getter;

public class Buffer {
    private FileMgr fileMgr;
    private LogMgr logMgr;

    @Getter
    private Page contents;

    @Getter
    private BlockId block = null; // Checking for Null is an atrocity and I hate it. Gonna add a "has block" function

    @Getter
    private int pins = 0;

    @Getter
    private long lastTimePinned = 0L;

    @Getter
    private long lastTimeUnpinned = 0L;

    @Getter
    private long modifyingTxNum = -1L;
    private long lsn = -1L;

    public Buffer(FileMgr fileMgr, LogMgr logMgr) {
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;

        contents = new Page(fileMgr.getBlocksize());
    }

    public void setModified(int modifyingTxNum, long lsn) {
        this.modifyingTxNum = modifyingTxNum;
        if (lsn >= 0)
            this.lsn = lsn;
    }

    public boolean isPinned() {
        return pins > 0;
    }

    protected void assignToBlock(BlockId block) {
        flush();
        this.block = block;
        fileMgr.readFromDiskToPage(block, contents);
        pins = 0;
    }

    // Not the same as the logMgr flush(), but very similar. Writes the contents of the page to the disk
    protected void flush() {
        if (modifyingTxNum >= 0) {
            logMgr.flush(lsn);
            fileMgr.writePageToDisk(block, contents);
            modifyingTxNum = -1;
        }
    }

    protected void pin() {
        pins++;

        lastTimePinned = System.currentTimeMillis();
    }

    protected void unpin() {
        pins--;

        if (pins == 0)
            lastTimeUnpinned = System.currentTimeMillis();
    }
}
