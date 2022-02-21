package Buffer;

import java.util.concurrent.atomic.AtomicInteger;

import File.BlockId;
import File.FileMgr;
import Log.LogMgr;
import Error.BufferException;

/**
 * Implements the niave buffer replacement strategy of simpleDB. Child classes of this one will
 * override and implement different/better functions depending on their buffer replacement strategy.
 */
public class BufferMgr {
    protected FileMgr fileMgr;
    protected LogMgr logMgr;
    protected AtomicInteger numAvailableBuffs;
    protected Buffer[] bufferPool;
    protected static final long MAX_WAIT_TIME = 10000; // 10 SECONDS

    /**
     * Default constructor for inheritance.
     */
    public BufferMgr() {

    }

    /**
     * Constructor for buffer manager. Only 1 buffer manager is created
     * during DB startup.
     *  @param fileMgr File manager to read blocks from disk into pages
     * @param logMgr Log manager to log events in case of system crash.
     * @param numBuffers The size of the buffer pool.
     */
    public BufferMgr(FileMgr fileMgr, LogMgr logMgr, int numBuffers) {
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;
        this.numAvailableBuffs = new AtomicInteger(numBuffers);
    }

    /**
     * Returns the number of available unpinned buffers in the buffer pool.
     */
    public int numAvailableBuffers() {
        return numAvailableBuffs.intValue();
    }

    /**
     * Flushes all of the buffers in the BufferMgr to the disk. Is synchronized to avoid
     * modifications to get into the pages from concurrent writes from clients
     * @param txNum PUT THIS HERE WHEN I FIGURE IT OUT
     */
    public synchronized void flushAll(int txNum) {
        for(Buffer buffer : bufferPool)
            if (buffer.modifyingTx() == txNum)
                buffer.flush();
    }

    /**
     * Unpins the page from the specified buffer. Since multiple clients can pin a page
     * to the buffer, this does not guarantee that the buffer is completely unpinned
     * and eligible for replacement. If the buffer is completely unpinned, the BufferMgr
     * updates the number of buffers eligible for replacement.
     * @param buffer The buffer to unpin the page from
     */
    public synchronized void unPin(Buffer buffer) {
        buffer.unPin();

        if (!buffer.isPinned()) {
            numAvailableBuffs.incrementAndGet();
            notifyAll();
        }
    }

    public Buffer pin(BlockId blk) throws BufferException {
        return null;
    }

    /**
     * Calculates if a client has been waiting too long. If client has been waiting too long,
     * client should be notified with an Error.BufferAbortException.
     * @param startTimeMillis The time the client started waiting for a page to become available to pin
     * @return true if client has waited too long, false otherwise
     */
    private boolean hasWaitedTooLong(long startTimeMillis) {
        return System.currentTimeMillis() - startTimeMillis > MAX_WAIT_TIME;
    }


}