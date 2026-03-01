package buffer;

import file.BlockId;
import file.FileMgr;
import log.LogMgr;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class RingBufferMgrStrategy extends BufferMgr {

    // Needed to keep track of where we are in the array so we can keep track of where we are and circle back to
    // the beginning if we reach the end
    private int ringBufferIndex = Integer.MIN_VALUE;

    private ArrayList<Buffer> bufferPool;

    public RingBufferMgrStrategy(FileMgr fileMgr, LogMgr logMgr, int numBuffers) {
        this.fileMgr = fileMgr;

        // Needed for the parent class to compute statistics
        numTotalBuffers = numBuffers;

        bufferPool = new ArrayList<>();

        numAvailableBuffers = new AtomicInteger(numBuffers);
        bufferBlockLUT = new HashMap<>();

        for (int i = 0; i < numBuffers; i++) {
            Buffer buff = new Buffer(fileMgr, logMgr);
            buff.setPoolIndex(i);
            bufferPool.add(buff);
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
            bufferBlockLUT.put(block.blockNum(), buffer);
        } else {
            buffer = attemptFindExisting.value();
        }

        if (!buffer.isPinned())
            numAvailableBuffers.decrementAndGet();

        buffer.pin();

        return Attempt.succeeded(buffer);
    }

    protected Attempt<Buffer> chooseUnPinnedBuffer() {
        // If this is the first iteration of the Ring strategy, initialize our index placeholder
        if (ringBufferIndex == Integer.MIN_VALUE)
            ringBufferIndex = 0;

        int lastIndex = bufferPool.size() - 1;
        int currentIndex = ringBufferIndex;
        Buffer currBuffer = bufferPool.get(ringBufferIndex);

        // If the first element we looked at is unpinned, then return it. If not, start cycling through the ring
        if (!currBuffer.isPinned()) {
            ringBufferIndex = getNextIndex(currentIndex);
            return Attempt.succeeded(currBuffer);
        }

        while (currBuffer.isPinned()) {
            currentIndex++;

            // If we get to the end, cycle back to the beginning
            if (currentIndex > lastIndex)
                currentIndex = 0;

            // We cycled through the entire buffer and found nothing
            if (currentIndex == ringBufferIndex)
                return Attempt.failed();

            currBuffer = bufferPool.get(currentIndex);
            if (!currBuffer.isPinned())
                break;
        }
        // If we exited the loop, then that means we found a buffer to return. Otherwise, we would've returned from
        // this function with Attempt.failed()

        // We want to make sure that we start from the next buffer in the buffer pool instead of this current one
        // the next time we look for an unpinned buffer
        ringBufferIndex = getNextIndex(currentIndex);
        return Attempt.succeeded(currBuffer);
    }

    private int getNextIndex(int currentIndex) {
        int lastIndex = bufferPool.size() - 1;
        int nextIndex = currentIndex + 1;
        return (nextIndex > lastIndex) ? 0 : nextIndex;
    }
}
