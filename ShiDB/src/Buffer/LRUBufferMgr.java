package Buffer;

import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Random;

import File.BlockId;
import File.FileMgr;
import Log.LogMgr;

import Error.BufferAbortException;

/**
 * Class for the buffer manager that uses the Least Recenty Used (LRU)
 * buffer replacement strategy. This class is nearly identical to the
 * {@link FIFOBufferMgr} class. The only difference is instead of a normal
 * queue, this class uses a priority queue with the comparator operation being
 * done with the lsn of the buffer that's pinned. Lower lsn means the buffer is
 * older, and is a better candidate for replacement.
 *
 * "Choose the unpinned buffer whose contents were unpinned least recently"
 */
public class LRUBufferMgr extends BufferMgr {

    private PriorityBlockingQueue bufferPool;
    private HashMap<Integer, Buffer> buffersInUse;

    /**
     * Constructor for buffer manager. Only 1 buffer manager is created
     * during DB startup.
     *
     * @param fileMgr File manager to read blocks from disk into pages
     * @param logMgr Log manager to log events in case of system crash.
     * @param numBuffers The size of the buffer pool.
     */
    public LRUBufferMgr(FileMgr fileMgr, LogMgr logMgr, int numBuffers) {
        super(fileMgr, logMgr, numBuffers);

        Comparator<Buffer> comparator = new BufferFIFOComparator();
        bufferPool = new PriorityBlockingQueue(numBuffers, comparator);

        // need to keep track of pinned buffers in case a client wants to pin an existing block
        buffersInUse = new HashMap<>();

        for (int i = 0; i < numAvailableBuffs.get(); i++)
            bufferPool.add(new Buffer(fileMgr, logMgr));
    }

    /**
     * Gets the number of buffers that are currently available for pinning to a block.
     * @return The number of available buffers
     */
    public int numAvailableBuffers() {
        return bufferPool.size();
    }

    /**
     * Flushes all of the buffers in the BufferMgr to the disk. Is synchronized to avoid
     * modifications to get into the pages from concurrent writes from clients
     * @param txNum The transaction number that we need to flush up to
     */
    public void flushAll(int txNum) {
        Buffer buffer = null;
        while(bufferPool.iterator().hasNext()) {
            buffer = (Buffer) bufferPool.iterator().next();
            if (buffer.modifyingTx() == txNum) {
                buffer.flush();
            }
        }
    }

    /**
     * Unpins the page from the specified buffer. Since multiple clients can pin a page
     * to the buffer, this does not guarantee that the buffer is completely unpinned
     * and eligible for replacement.
     * @param buffer The buffer to unpin the page from
     */
    public void unPin(Buffer buffer) {
        buffer.unPin();

        if (!buffer.isPinned()) {
            bufferPool.add(buffer);
            buffersInUse.remove(buffer.getBlock().hashCode());
        }
    }


    /**
     * Attempts to pin the specified block/page into a buffer to be used by the client. Will
     * attempt to pin and wait until the specified MAX_WAIT_TIME has elapsed. If has waited
     * for longer than the MAX_WAIT_TIME, will return an exception.
     * @param blk The block that we are trying to read and pin into a buffer
     * @return The buffer that was read from the specified block
     */
    public Buffer pin(BlockId blk) {
        Buffer buffer = findExistingBuffer(blk);

        try {
            if (buffer == null) { // no buffer with same block pinned
                buffer = (Buffer) bufferPool.poll(super.MAX_WAIT_TIME, TimeUnit.MILLISECONDS);

                if(buffer == null)
                    throw new BufferAbortException("Client has waited too long. Aborting pin() operation!");
            }

            buffer.assignToBlock(blk);
            buffer.pin();

            if(!buffersInUse.containsKey(blk.hashCode()))
                buffersInUse.put(blk.hashCode(), buffer);

            return buffer;
        }
        catch(InterruptedException e) {
            throw new BufferAbortException("Client got interrupted while waiting. Aborting pin() operation!");
        }
    }

    /**
     * Checks the {@link HashMap} of buffers already in use to see if a
     * block is already pinned so we don't need to read in a new block from the disk. Don't
     * need to worry if a block is null because any block in the bufferInUse HashMap
     * will have a block pinned
     * @param blk The block we want to find
     * @return A buffer that has a matching block. Null otherwise.
     */
    private Buffer findExistingBuffer(BlockId blk) {
        if (buffersInUse.containsKey(blk.hashCode()))
            return buffersInUse.get(blk.hashCode());

        return null;
    }

}
