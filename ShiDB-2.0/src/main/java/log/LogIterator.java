package log;

import file.BlockId;
import file.FileMgr;
import file.Page;

import java.util.Iterator;

// This class is package private since only the LogMgr class should interact with it.
class LogIterator implements Iterator<byte[]> {
    private FileMgr fileMgr;
    private BlockId block;
    private Page page;
    private int currentPosition;
    private int boundary;

    public LogIterator(FileMgr fileMgr, BlockId block) {
        this.fileMgr = fileMgr;
        this.block = block;
        byte[] byteArr = new byte[fileMgr.getBlocksize()];
        page = new Page(byteArr);

        moveToBlock(block);
    }

    public boolean hasNext() {
        return currentPosition < fileMgr.getBlocksize() || block.blockNum() > 0;
    }

    public byte[] next() {
        // The Log iterator goes backwards in time, hence why we decrement the block number instead of increment
        if (currentPosition == fileMgr.getBlocksize()) {
            block = new BlockId(block.filename(), block.blockNum() - 1);
            moveToBlock(block);
        }

        byte[] record = page.getBytes((currentPosition));
        currentPosition += Integer.BYTES + record.length;

        return record;
    }

    public void moveToBlock(BlockId block) {
        fileMgr.readFromDiskToPage(block, page);
        boundary = page.getInt(0);
        currentPosition = boundary;
    }
}
