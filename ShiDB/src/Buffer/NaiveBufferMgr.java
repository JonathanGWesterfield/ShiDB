package Buffer;


import File.BlockId;
import File.FileMgr;
import Log.LogMgr;

import Error.BufferAbortException;

public class NaiveBufferMgr extends BufferMgr {

    private Buffer[] bufferPool;

    /**
     * Constructor for buffer manager. Only 1 buffer manager is created
     * during DB startup.
     *  @param fileMgr File manager to read blocks from disk into pages
     * @param logMgr Log manager to log events in case of system crash.
     * @param numBuffers The size of the buffer pool.
     */
    public NaiveBufferMgr(FileMgr fileMgr, LogMgr logMgr, int numBuffers) {
        super(fileMgr, logMgr, numBuffers);

        bufferPool = new Buffer[numAvailableBuffs.intValue()];
        for (int i = 0; i < bufferPool.length; i++)
            bufferPool[i] = new Buffer(this.fileMgr, this.logMgr);
    }



    /**
     * Returns the number of available unpinned buffers in the buffer pool.
     */
    @Override
    public int numAvailableBuffers() {
        return super.numAvailableBuffs.intValue();
    }

    /**
     * Flushes all of the buffers in the BufferMgr to the disk. Is synchronized to avoid
     * modifications to get into the pages from concurrent writes from clients
     * @param txNum PUT THIS HERE WHEN I FIGURE IT OUT
     */
    @Override
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
    @Override
    public synchronized void unPin(Buffer buffer) {
        buffer.unPin();

        if (!buffer.isPinned()) {
            numAvailableBuffs.incrementAndGet();
            notifyAll();
        }
    }

    /**
     * Attempts to pin the specified block/page into a buffer to be used by the client. Will
     * attempt to pin and wait until the specified MAX_WAIT_TIME has elapsed. If has waited
     * for longer than the MAX_WAIT_TIME, will return an exception.
     * @param blk The block that we are trying to read and pin into a buffer
     * @return The buffer that was read from the specified block
     */
    @Override
    public synchronized Buffer pin(BlockId blk) {

        long startTimestamp = System.currentTimeMillis();

        Buffer buffer = attemptToPin(blk);
        while(buffer == null && !hasWaitedTooLong(startTimestamp)) {
            //                wait(500);
            buffer = attemptToPin(blk);
        }

        if(buffer == null)
            throw new BufferAbortException("Client has waited too long. Aborting pin() operation!");

        return buffer;
    }

    /**
     * Attempts to pin the specified block to a buffer. Will attempt to search the available buffers
     * for a buffer that coincidentally has the exact same block pinned. If no block is found, will
     * attempt to pin the block to an available unpinned buffer. If that still doesn't work, will
     * return null so the client can wait for a buffer to become available.
     * @param blk The block we want to pin to the buffer.
     * @return A successfully pinned buffer. Null if no buffer is available for pinning.
     */
    private Buffer attemptToPin(BlockId blk) {
        Buffer buffer = findExistingBuffer(blk);

        if (buffer == null) { // no buffer with same block pinned
            buffer = chooseUnpinnedBuffer();
            if (buffer == null) // no available buffers for new block to pin to
                return null;
            buffer.assignToBlock(blk);
        }

        // if buffer isn't pinned, we need to pin it to remove it from available buffers to replace
        if(!buffer.isPinned())
            numAvailableBuffs.decrementAndGet();
        buffer.pin();

        return buffer;
    }

    private Buffer findExistingBuffer(BlockId blk) {
        for (Buffer buffer : bufferPool)
            if (buffer.getBlock() != null && buffer.getBlock().equals(blk))
                return buffer;

        return null;
    }

    /**
     * Niavely tries to find an unpinned buffer by simply parsing through the buffer pool
     * array.
     * @return An unpinned buffer. Null if no unpinned buffer was found.
     */
    private Buffer chooseUnpinnedBuffer() {
        for (Buffer buffer : bufferPool)
            if(!buffer.isPinned())
                return buffer;

        return null; // indicates that there was no available buffer
    }

    /**
     * Calculates if a client has been waiting too long. If client has been waiting too long,
     * client should be notified with an Error.BufferAbortException.
     * @param startTimeMillis The time the client started waiting for a page to become available to pin
     * @return true if client has waited too long, false otherwise
     */
    protected boolean hasWaitedTooLong(long startTimeMillis) {
        return System.currentTimeMillis() - startTimeMillis > MAX_WAIT_TIME;
    }


}
