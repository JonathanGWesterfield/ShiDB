package Buffer;

import java.util.concurrent.atomic.AtomicInteger;

import File.BlockId;
import File.FileMgr;
import Log.LogMgr;

/**
 * Implements the niave buffer replacement strategy of simpleDB. Child classes of this one will
 * override and implement different/better functions depending on their buffer replacement strategy.
 */
public abstract class BufferMgr {
    protected FileMgr fileMgr;
    protected LogMgr logMgr;
    protected AtomicInteger numAvailableBuffs;
    protected static final long MAX_WAIT_TIME = 10000; // 10 SECONDS

    /**
     * Default Constructor
     */
    public BufferMgr() {}

    /**
     * Constructor. Populates the buffer pool with array of buffers
     * @param fileMgr
     * @param logMgr
     * @param numBuffers
     */
    public BufferMgr(FileMgr fileMgr, LogMgr logMgr, int numBuffers) {
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;
        this.numAvailableBuffs = new AtomicInteger(numBuffers);

        return; // unnecessary return statement for debugging
    }

    /**
     * Gets the number of buffers that are currently available for pinning to a block.
     * @return The number of available buffers
     */
    abstract int numAvailableBuffers();

    /**
     * Flushes all of the buffers in the BufferMgr to the disk. Is synchronized to avoid
     * modifications to get into the pages from concurrent writes from clients
     * @param txNum The transaction number that we need to flush up to
     */
    abstract void flushAll(int txNum);

    /**
     * Unpins the page from the specified buffer. Since multiple clients can pin a page
     * to the buffer, this does not guarantee that the buffer is completely unpinned
     * and eligible for replacement.
     * @param buffer The buffer to unpin the page from
     */
    abstract void unPin(Buffer buffer);


    /**
     * Attempts to pin the specified block/page into a buffer to be used by the client. Will
     * attempt to pin and wait until the specified MAX_WAIT_TIME has elapsed. If has waited
     * for longer than the MAX_WAIT_TIME, will return an exception.
     * @param blk The block that we are trying to read and pin into a buffer
     * @return The buffer that was read from the specified block
     */
    abstract Buffer pin(BlockId blk);
}