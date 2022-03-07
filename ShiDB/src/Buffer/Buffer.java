package Buffer;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import File.Page;
import File.FileMgr;
import File.BlockId;
import Log.LogMgr;
import Error.EnumBufferError;
import Error.BufferException;


/**
 * Comparator class to be used for priority queues (in the {@link FIFOBufferMgr}).
 * Enables priority queue use with the {@link Buffer} class.
 */
class BufferFIFOComparator implements Comparator<Buffer> {

    /**
     * Compare method needed for the comparator interface.
     * @param buffer1 1st buffer to compare to 2nd buffer
     * @param buffer2 2nd buffer to compare to 1st buffer
     * @return -1 if 1st is less than second, 0 if equal, 1 if 1st is greater
     */
    public int compare(Buffer buffer1, Buffer buffer2) {
        return buffer1.compareTimeReadIn(buffer2);
    }
}

/**
 * Comparator class to be used for priority queues (in the {@link LRUBufferMgr}).
 * Enables priority queue use with the {@link Buffer} class.
 */
class BufferLRUComparator implements Comparator<Buffer> {

    public int compare(Buffer buffer1, Buffer buffer2) {
        return buffer1.compareTimeUnpinned(buffer2);
    }
}

/**
 * Comparator class to be used for priority queues (in the {@link LsnBufferMgr}).
 * Enables priority queue use with the {@link Buffer} class.
 */
class BufferLsnComparator implements Comparator<Buffer> {
    public int compare(Buffer buffer1, Buffer buffer2) {
        return buffer1.compareLsn(buffer2);
    }
}

/**
 * Buffer holds a page and a block that has been read into that page. Also keeps track
 * of what clients currently have the buffer pinned and the modifying transaction number and
 * log sequence number of the modifying transaction
 */
public class Buffer  {

    private FileMgr fileMgr;
    private LogMgr logMgr;
    private AtomicInteger pins;
    private Page contents;
    private int txNum;
    private BlockId block;
    private int lsn;
    private long timeUnpinned;
    private long timeReadIn;

    public Buffer(FileMgr fileMgr, LogMgr logMgr) {
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;
        pins = new AtomicInteger(0);
        txNum = -1; // if -1, means no change to this block
        lsn = -1;
        contents = new Page(fileMgr.getBlockSize());
    }

    /**
     * Returns the Page assigned to this buffer.
     * @return
     */
    public Page getContents() {
        return contents;
    }

    /**
     * Gives the block assigned to this buffer.
     * @return The block assigned to this buffer, if there is one.
     */
    public BlockId getBlock() {
        return block;
    }

    /**
     * Set's whether the page under this buffer has been modified. Modified pages should be
     * written to the disk before they are replaced with another block.
     * @param txNum The number of the transaction that modified the page attached to this buffer.
     * @param lsn The log sequence number of the transaation that modified the page under this buffer.
     */
    public void setModified(int txNum, int lsn) {
        this.txNum = txNum;
        if (lsn >= 0)
            this.lsn = lsn;
    }

    /**
     * Indicates whether the buffer currently has a page pinned to it.
     * @return true if pinned, false otherwise
     */
    public boolean isPinned() {
        return pins.get() > 0;
    }

    /**
     * Logs the modified records, writes the contents of the modified page to the disk,
     * and then resets the transaction number to set the block as "unmodified"
     */
    protected void flush() {
        if (txNum >= 0) {
            logMgr.flush(lsn);
            fileMgr.write(block, contents);
            txNum = -1; // set to unmodified for the next block
        }
    }

    /**
     * The underlying mechanism for pinning a page to a block. Reads the specified block from
     * the disk and stores it in the page of this buffer object. If the page that will be
     * replaced by this method has been modified, it will be written to the disk first before
     * the new block is read in.
     * @param block The specific block to read from the disk
     */
    protected void assignToBlock(BlockId block) {
        flush();
        this.block = block;
        fileMgr.read(block, contents);
        pins.set(0);
        timeReadIn = System.currentTimeMillis();
    }

    /**
     * Get's the modifying transaction number of this block/page. If set to -1, means
     * that this block/page is unmodified.
     * @return
     */
    public int modifyingTx() {
        return txNum;
    }

    /**
     * Sets whether page is pinned to the buffer or not. Increments the pin based
     * on number of clients pinning this page.
     * @return The number of pins the buffer has after being incremented
     */
    public int pin() {
        return pins.incrementAndGet();
    }

    /**
     * Decrements the number of pins for the page under this buffer
     * @return The number of pins the page has after decrementing
     * @throws Exception Exception if the unpin is called when there are no pins left
     */
    public int unPin() {
        if (pins.get() < 1) {
            throw new BufferException(EnumBufferError.CANNOT_UNPIN_BUFFER_HAS_NO_PINS.toString());
        }

        pins.decrementAndGet();

        if (!this.isPinned())
            timeUnpinned = System.currentTimeMillis();

        return pins.get();
    }

    /**
     * Gets the age of the block read into the buffer. Used for the FIFO replacement strategy
     * that uses the "time read in" value to replace the oldest buffer.
     * @return
     */
    public long getTimeReadIn() {
        return timeReadIn;
    }

    /**
     * Gets the time that this buffer was last completely unpinned. Used for the LRU
     * replacement strategy .
     * @return
     */
    public long getTimeUnpinned() {
        return timeUnpinned;
    }

    /**
     * Returns the log sequence number of the modifying transaction. Used to determine
     * the most recent change to the buffer for replacement strategies
     * @return The lsn of the modifying transaction. -1 if unmodified
     */
    public int getLsn() {
        return lsn;
    }

    /**
     * Used for the comparator needed for a priority queue (used in {@link LRUBufferMgr})
     * @param obj Buffer object to compare against
     * @return -1 if smaller than obj, 0 if equal, 1 if greater than.
     */
    public int compareTimeUnpinned(Object obj) {
        Buffer buffer = (Buffer)obj;

        return (timeUnpinned < buffer.getTimeUnpinned()) ? -1 : ((timeUnpinned == buffer.getTimeUnpinned()) ? 0 : 1);
    }

    /**
     * Used for the comparator needed for a priority queue (used in {@link FIFOBufferMgr})
     * which compares the buffer by the time read in
     * @param obj Buffer object to compare against
     * @return -1 if smaller than obj, 0 if equal, 1 if greater than.
     */
    public int compareTimeReadIn(Object obj) {
        Buffer buffer = (Buffer)obj;

        return (timeReadIn < buffer.getTimeReadIn()) ? -1 : ((timeReadIn == buffer.getTimeReadIn()) ? 0 : 1);
    }

    /**
     * Used for the comparator needed for a priority queue (used in {@link LsnBufferMgr})
     * which compares the buffer by the log sequence number and whether buffer is unmodified or not.
     * @param obj Buffer object to compare against
     * @return -1 if smaller than obj, 0 if equal, 1 if greater than.
     */
    public int compareLsn(Object obj) {
        Buffer buffer = (Buffer) obj;

        return (lsn < buffer.getLsn()) ? -1 : ((lsn == buffer.getLsn()) ? 0 : 1);
    }
}


