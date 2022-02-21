package Buffer;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import File.Page;
import File.FileMgr;
import File.BlockId;
import Log.LogMgr;
import Error.EnumBufferError;
import Error.BufferException;

public class Buffer {

    private FileMgr fileMgr;
    private LogMgr logMgr;
    private AtomicInteger pins;
    private Page contents;
    private int txNum;
    private BlockId block;
    private int lsn;

    public Buffer(FileMgr fileMgr, LogMgr logMgr) {
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;
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
     * Gives and Optional of the block assigned to this buffer. Block is optional because
     * the block could be null.
     * @return The block assigned to this buffer, if there is one.
     */
    public Optional<BlockId> getBlock() {
        return Optional.of(block);
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
    private void assignToBlock(BlockId block) {
        flush();
        this.block = block;
        fileMgr.read(block, contents);
        pins.set(0);
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

        return pins.decrementAndGet();
    }
}