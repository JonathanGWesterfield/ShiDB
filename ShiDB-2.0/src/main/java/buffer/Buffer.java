package buffer;

import file.BlockId;
import file.FileMgr;
import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.Setter;

public class Buffer {
    private FileMgr fileMgr;
    private LogMgr logMgr;

    @Getter
    private Page contents;

    @Getter
    private BlockId block = null; // Checking for Null is an atrocity and I hate it. Gonna add a "has block" function

    @Getter
    private int pins = 0;

    // These timestamps need to be in nanoseconds in case buffers get pinned very quickly right after each other
    // System.currentTimeMillis doesn't have enough granularity when things go fast enough (I found this out through
    // unit testing where algorithms wouldn't work because the timestamps were literally identical in milliseconds)
    @Getter
    private long lastTimePinnedNano = Long.MIN_VALUE;

    @Getter
    private long lastTimeUnpinnedNano = Long.MIN_VALUE;

    // Temp variable to help me figure out wtf is going on in unit testing
    @Getter @Setter
    private int poolIndex;

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

        lastTimePinnedNano = System.nanoTime();
    }

    protected void unpin() {
        pins--;

        if (pins == 0)
            lastTimeUnpinnedNano = System.nanoTime();
    }
}
