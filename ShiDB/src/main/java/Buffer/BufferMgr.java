package Buffer;

import File.Page;
import File.BlockId;
import File.FileMgr;
import Log.LogMgr;

public class BufferMgr {

    private FileMgr fileMgr;
    private LogMgr logMgr;
    private int numBuffers;

    /**
     * Constructor for buffer manager. Only 1 buffer manager is created
     * during DB startup.
     *
     * @param fileMgr File manager to read blocks from disk into pages
     * @param logMgr Log manager to log events in case of system crash.
     * @param numBuffers The size of the buffer pool.
     */
    public BufferMgr(FileMgr fileMgr, LogMgr logMgr, int numBuffers) {
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;
        this.numBuffers = numBuffers;

    }

    public Buffer pin(BlockId blk) {
        return null;
    }

    public void unPin(Buffer buffer) {

    }

    /**
     * Returns the number of available unpinned buffers in the buffer pool.
     */
    public int numAvailableBuffers() {
        return 0;
    }

    public void flushAll(int txNum) {

    }
}