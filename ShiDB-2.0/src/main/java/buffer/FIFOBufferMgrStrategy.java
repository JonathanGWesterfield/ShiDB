package buffer;

import file.BlockId;
import file.FileMgr;
import log.LogMgr;

import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class FIFOBufferMgrStrategy extends BufferMgr {

    PriorityQueue<Buffer> freeBufferPool;

    // Since the freeBufferPool literally just pops things out, we need to keep track of which buffers we loaned out
    HashMap<BlockId, Buffer> inUseBufferMap;

    // Comparator to make sure our priority queue returns the least recently pinned buffers first
    Comparator<Buffer> leastRecentlyPinned = new Comparator<Buffer>() {
        @Override
        public int compare(Buffer buff1, Buffer buff2) {
            // In real life, it doesn't matter, but for the sake of unit tests, need this statement to ensure that
            // at the very first pin for each buffer, the selection is deterministic and goes in an expected order
            // of the buffers. Without this, the order of buffer selection for the first time the buffer is pinned
            // is effectively random.
            if (buff1.getLastTimePinnedNano() == Long.MIN_VALUE && buff2.getLastTimePinnedNano() == Long.MIN_VALUE)
                return 1;

            return (int) (buff1.getLastTimePinnedNano() - buff2.getLastTimePinnedNano());
        }
    };

    public FIFOBufferMgrStrategy(FileMgr fileMgr, LogMgr logMgr, int numBuffers) {
        this.fileMgr = fileMgr;

        // Needed for the parent class to compute statistics
        numTotalBuffers = numBuffers;

        freeBufferPool = new PriorityQueue<>(leastRecentlyPinned);

        // There's a good chance we may not need to keep track of buffers in use, but it's a big footgun if we just
        // decide that clients will always properly return/unpin their buffers
        inUseBufferMap = new HashMap<>();
        numAvailableBuffers = new AtomicInteger(numBuffers);
        bufferBlockLUT = new HashMap<>();

        for (int i = 0; i < numBuffers; i++) {
            Buffer buff = new Buffer(fileMgr, logMgr);
            buff.setPoolIndex(i);
            freeBufferPool.offer(buff);
        }
    }

    @Override
    public synchronized void flushAll(long txNum) {
        // Need to flush but the free pool and the inUse pool in case the transaction num matches (aka dirty buffers)
        for (Buffer buff : freeBufferPool)
            if (buff.getModifyingTxNum() == txNum)
                buff.flush();

        for (Buffer buff : inUseBufferMap.values())
            if (buff.getModifyingTxNum() == txNum)
                buff.flush();
    }

    @Override
    public synchronized void unpinBuffer(Buffer buffer) {
        buffer.unpin();
        if (!buffer.isPinned()) {
            // Re add it back to the freeBufferPool
            freeBufferPool.offer(buffer);

            if (buffer.getBlock() != null)
                inUseBufferMap.remove(buffer.getBlock());

            numAvailableBuffers.incrementAndGet();
            notifyAll();
        }

        bufferCountSanityCheck();
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
            inUseBufferMap.put(block, buffer);
        }
        else {
            buffer = attemptFindExisting.value();
        }

        if (!buffer.isPinned())
            numAvailableBuffers.decrementAndGet();

        buffer.pin();

        bufferCountSanityCheck();

        return Attempt.succeeded(buffer);
    }

    private void bufferCountSanityCheck() {
        int trackedBuffers = inUseBufferMap.size() + freeBufferPool.size();
        if (trackedBuffers != numTotalBuffers) {
            String errMsg = String.format("Free Buffer Count: %d -- In Use Buffer Count: %d -- " +
                            "Total Num Expected Buffers: %d -- Missing Num Buffers: %d",
                    freeBufferPool.size(), inUseBufferMap.size(), numTotalBuffers, numTotalBuffers - trackedBuffers);

            throw new RuntimeException(String.format("Somewhere along the way, we lost track of a buffer! %s", errMsg));
        }
    }

    @Override
    protected Attempt<Buffer> chooseUnPinnedBuffer() {
        if (freeBufferPool.isEmpty())
            return Attempt.failed();

        Buffer foundBuffer = freeBufferPool.poll();

        if (foundBuffer.isPinned())
            throw new RuntimeException("Found buffer was still somehow pinned!");


        // Keeping here for experimentation. If we confirmed there is an unpinned buffer, then being unable to pin
        // is a serious error, but the book doesn't account for this. I'll keep this around in case I want to change it
        // throw new RuntimeException("We thought there was an unpinned buffer to use, but somehow, we are wrong!");

        return Attempt.succeeded(foundBuffer);
    }
}
