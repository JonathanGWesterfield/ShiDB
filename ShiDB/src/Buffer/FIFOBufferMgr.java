package Buffer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import File.BlockId;
import File.FileMgr;
import Log.LogMgr;
import Error.BufferAbortException;

/**
 * To refer to what each function does at a high level, refer to the {@link BufferMgr}
 * class. Javadoc strings for functions in this class will only describe what
 * each function does specific to the FIFO replacement strategy.
 *
 * "Chooose the unpinned buffer whose contents were *replaced* least recently"
 */
public class FIFOBufferMgr extends BufferMgr {

    /**
     * Using a {@link LinkedBlockingQueue} instead of a {@link LinkedList} for
     * thread safety
     */
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
    public FIFOBufferMgr(FileMgr fileMgr, LogMgr logMgr, int numBuffers) {
        super(fileMgr, logMgr, numBuffers);

        BufferFIFOComparator comparator = new BufferFIFOComparator();
        bufferPool = new PriorityBlockingQueue(numBuffers, comparator);

        // need to keep track of pinned buffers in case a client wants to pin an existing block
        buffersInUse = new HashMap<>();

        for (int i = 0; i < numBuffers; i++)
            bufferPool.add(new Buffer(this.fileMgr, this.logMgr));
    }

    /**
     * Returns the number of available unpinned buffers in the buffer pool by getting
     * the number of buffers available in the queue
     * @return The number of available buffers that can be pinned in the queue
     */
    @Override
    public int numAvailableBuffers(){
        return bufferPool.size();
    }

    /**
     * Flushes all of the buffers in the BufferMgr to the disk.
     * @param txNum PUT THIS HERE WHEN I FIGURE IT OUT
     */
    @Override
    public synchronized void flushAll(int txNum) {
        Buffer buffer = null;
        while(bufferPool.iterator().hasNext()) {
            buffer = (Buffer) bufferPool.iterator().next();
            if (buffer.modifyingTx() == txNum) {
                buffer.flush();
            }
        }
    }

    /**
     * Unpins the buffer for the client. Buffer can still be pinned if another client
     * has a pin. Doesn't guarantee a buffer will be freed up. If buffer is fully unpinned
     * after this operation, buffer is added to the tail of the availableBuffers queue.
     * @param buffer The buffer to unpin the page from
     */
    @Override
    public synchronized void unPin(Buffer buffer) {
        buffer.unPin();

        if (!buffer.isPinned()) {
            bufferPool.add(buffer);
            buffersInUse.remove(buffer.getBlock().hashCode());
            notifyAll();
        }
    }

    /**
     * Attempts to find a Buffer to pin the block to. Will first try to find a buffer
     * that already has the block pinned. If none are found, will try to pull buffer from
     * the availableBuffers queue and pin it. Will throw {@link BufferAbortException} if
     * no availableBuffer is provided within the MAX_WAIT_TIME.
     * @param blk The block we want to pin to the buffer.
     * @return
     */
    @Override
    public synchronized Buffer pin(BlockId blk) {
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

// linked list .poll() to pull from front of queue
// linked list .add() to add to back of queue
