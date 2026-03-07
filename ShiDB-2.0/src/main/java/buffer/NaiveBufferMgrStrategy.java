package buffer;

import file.BlockId;
import file.FileMgr;
import log.LogMgr;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class NaiveBufferMgrStrategy extends BufferMgr {
    LinkedList<Buffer> bufferPool;

    public NaiveBufferMgrStrategy(FileMgr fileMgr, LogMgr logMgr, int numBuffers) {
        this.fileMgr = fileMgr;

        // Needed for the parent class to compute statistics
        numTotalBuffers = numBuffers;

        bufferPool = new LinkedList<>();
        numAvailableBuffers = new AtomicInteger(numBuffers);
        bufferBlockLUT = new HashMap<>();

        for (int i = 0; i < numBuffers; i++) {
            Buffer buff = new Buffer(fileMgr, logMgr);
            buff.setPoolIndex(i);
            bufferPool.offer(buff);
        }
    }

    @Override
    public synchronized void flushAll(long txNum) {
        for (Buffer buff : bufferPool)
            if (buff.getModifyingTxNum() == txNum)
                buff.flush();
    }

    @Override
    public synchronized void unpinBuffer(Buffer buffer) {
        buffer.unpin();
        if (!buffer.isPinned()) {
            // Re add it back to the freeBufferPool
            bufferPool.offer(buffer);

            numAvailableBuffers.incrementAndGet();
            notifyAll();
        }
    }

    @Override
    protected Attempt<Buffer> tryToPin(BlockId block) {
        Attempt<Buffer> attemptFindExisting = findExistingBuffer(block);

        Buffer buffer;
        if (attemptFindExisting.hasFailed()) {
            Attempt<Buffer> attemptChooseUnpinnedBuffer = chooseUnPinnedBuffer();
            if (attemptChooseUnpinnedBuffer.hasFailed())
                return Attempt.failed();

            buffer = attemptChooseUnpinnedBuffer.value();

            // If the buffer already had a block mapped, it's no longer valid since we are going to reassign it
            // Need to update the mapping
            evictBlockFromMappedBuffers(buffer);

            buffer.assignToBlock(block);
            bufferBlockLUT.put(block, buffer);
        }
        else {
            buffer = attemptFindExisting.value();
        }

        if (!buffer.isPinned())
            numAvailableBuffers.decrementAndGet();

        buffer.pin();


        return Attempt.succeeded(buffer);
    }

    @Override
    protected Attempt<Buffer> chooseUnPinnedBuffer() {
        for (Buffer buffer : bufferPool) {
            if (!buffer.isPinned())
                return Attempt.succeeded(buffer);
        }

        // Keeping here for experimentation. If we confirmed there is an unpinned buffer, then being unable to pin
        // is a serious error, but the book doesn't account for this. I'll keep this around in case I want to change it
        // throw new RuntimeException("We thought there was an unpinned buffer to use, but somehow, we are wrong!");

        return Attempt.failed();
    }
}
