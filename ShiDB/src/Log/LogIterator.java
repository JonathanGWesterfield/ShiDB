package Log;

import java.util.Iterator;

import File.BlockId;
import File.FileMgr;
import File.Page;

public class LogIterator implements Iterator<byte[]> {

    private FileMgr fileMgr;
    private BlockId currentBlk;
    private Page page;
    private int currentPos;
    private int pageBoundary;

    /**
     * Constructor of an iterator for log records.
     * @param fileMgr The file manager that this database is using. Need it for blocksize.
     * @param startBlk The starting block we need records at. All blocks after this will be retrieved
     *                 in descending order (newest to oldest blocks).
     */
    protected LogIterator(FileMgr fileMgr, BlockId startBlk) {
        this.fileMgr = fileMgr;
        this.currentBlk = startBlk;
        byte[] bArr = new byte[this.fileMgr.getBlockSize()];
        this.page = new Page(bArr); // empty page requires empty array to be wrapped
        moveToBlock(startBlk);
    }

    /**
     * Determines whether or not there is another record to show. Does this by seeing if there
     * is more records in the page, or if we have more blocks to look through in the log file.
     * @return True if there are more records to retrieve. False otherwise.
     */
    public boolean hasNext() {
        return (currentPos < fileMgr.getBlockSize()) || (currentBlk.getBlkNum() > 0);
    }

    /**
     * Gives the next record. Does this by either moving to a new block when the end
     * of the current block has been reached or by getting the next record from the
     * current page that's been read from the disk.
     * @return The next log record in the log file in ascending order by date (newest to oldest records).
     */
    public byte[] next() {
        if (currentPos == fileMgr.getBlockSize()) { // reached end of a block
            currentBlk = new BlockId(currentBlk.getFilename(), currentBlk.getBlkNum() - 1);
            moveToBlock(currentBlk);
        }
        byte[] record = page.getBytes(currentPos);
        currentPos += Integer.BYTES + record.length;
        return record;
    }

    /**
     * Moves to a new block, reads the block into memory and gets the buffer pointer for
     * starting records in the page.
     * @param blk The block we want the iterator to read from in the log file.
     */
    private void moveToBlock(BlockId blk) {
        fileMgr.read(blk, page);
        pageBoundary = page.getInt(0);
        currentPos = pageBoundary;
    }
}
