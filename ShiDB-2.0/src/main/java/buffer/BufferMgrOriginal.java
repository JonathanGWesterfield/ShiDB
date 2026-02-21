package buffer;

import error.BufferAbortException;
import file.BlockId;
import file.FileMgr;
import file.Size;
import log.LogMgr;
import lombok.Getter;
import lombok.Setter;
import server.ConfigFetcher;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferMgrOriginal {

    // Adding a getter to assist with unit testing, but no other package should ever directly access this member
    @Getter
    private ArrayList<Buffer> bufferPool;

    private AtomicInteger numAvailableBuffers;
    
    private FileMgr fileMgr;

    @Getter @Setter
    private BufferSelectionStrategy unpinnedBufferselectionStrategy;

    // Only used if the buffer choice strategy is the RING buffer strategy
    // Needed to keep track of where we are in the array so we can keep track of where we are and circle back to
    // the beginning if we reach the end
    private int ringBufferIndex = Integer.MIN_VALUE;

    private static final long MAX_TIME_WAIT_FOR_PIN_MILLISECONDS = ConfigFetcher.getBufferMgrMaxWaitTime();
    private static final long WAIT_TIME_STEP_MILLISECONDS = ConfigFetcher.getBufferMgrPollStepTime();

    public BufferMgrOriginal(FileMgr fileMgr, LogMgr logMgr, int numBuffers) {
        unpinnedBufferselectionStrategy = ConfigFetcher.getBufferMgrSelectionStrategy();
        bufferPool = new ArrayList<>();
        numAvailableBuffers = new AtomicInteger(numBuffers);

        for (int i = 0; i < numBuffers; i++) {
            Buffer buff = new Buffer(fileMgr, logMgr);
            bufferPool.add(buff);
        }
    }
    
    public long getTotalSpaceOfBufferPool(Size size) {
        int numBytes = bufferPool.size() * fileMgr.getBlocksize();

        switch (size) {
            case Size.MEGABYTES -> { return numBytes / 1000; }
            case Size.GIGABYTES -> { return numBytes / 1000000; }
            default -> { return numBytes; }
        }
    }

    public long getCurrentlyUsedBufferSpace(Size size) {
        int numBytes = (bufferPool.size() - numAvailableBuffers.intValue()) * fileMgr.getBlocksize();

        switch (size) {
            case Size.MEGABYTES -> { return numBytes / 1000; }
            case Size.GIGABYTES -> { return numBytes / 1000000; }
            default -> { return numBytes; }
        }
    }

    public long getAvailableBufferSpace(Size size) {
        int numBytes = numAvailableBuffers.intValue() * fileMgr.getBlocksize();

        switch (size) {
            case Size.MEGABYTES -> { return numBytes / 1000; }
            case Size.GIGABYTES -> { return numBytes / 1000000; }
            default -> { return numBytes; }
        }
    }

    public int getNumAvailableBuffers() {
        return numAvailableBuffers.get();
    }

    public synchronized void flushAllBuffers(long modifyingTxNum) {
        bufferPool.stream()
                .filter(buffer -> buffer.getModifyingTxNum() != modifyingTxNum)
                .forEach(buffer -> buffer.flush());
    }

    public synchronized void unpinBuffer(Buffer buffer) {
        buffer.unpin();
        if (!buffer.isPinned()) {
            numAvailableBuffers.incrementAndGet();
            notifyAll();
        }
    }

    public synchronized Buffer pinBuffer(BlockId block) {
        try {
            long startTime = System.currentTimeMillis();
            Attempt<Buffer> attemptToPin = tryToPin(block);

            while (attemptToPin.hasFailed() && !hasWaitedTooLong(startTime)) {
                wait(WAIT_TIME_STEP_MILLISECONDS);
                attemptToPin = tryToPin(block);
            }

            if (attemptToPin.hasFailed())
                throw new BufferAbortException("Waited too long for a buffer to become available and timed out!");

            return attemptToPin.value();
        }
        catch (InterruptedException e) {
            throw new BufferAbortException("While waiting for a buffer to become available, thread was interrupted!");
        }
    }

    private Attempt<Buffer> tryToPin(BlockId block) {
        Attempt<Buffer> attemptFindExisting = findExistingBuffer(block);

        Buffer buffer;
        if (attemptFindExisting.hasFailed()) {
            Attempt<Buffer> attemptChooseUnpinnedBuffer = chooseUnPinnedBuffer();
            if (attemptChooseUnpinnedBuffer.hasFailed())
                return Attempt.failed();

            buffer = attemptChooseUnpinnedBuffer.value();
            buffer.assignToBlock(block);
        }
        else {
            buffer = attemptFindExisting.value();
        }

        if (!buffer.isPinned())
            numAvailableBuffers.decrementAndGet();

        buffer.pin();

        return Attempt.succeeded(buffer);
    }

    private Attempt<Buffer> findExistingBuffer(BlockId block) {
        for (Buffer buffer: bufferPool) {
            BlockId blk = buffer.getBlock();
            if (blk != null && blk == block)
                return Attempt.succeeded(buffer);
        }

        return Attempt.failed();
    }

    private Attempt<Buffer> chooseUnPinnedBuffer() {
        return switch (unpinnedBufferselectionStrategy) {
            case BufferSelectionStrategy.FIFO -> chooseUnpinnedBufferFIFOStrategy();
            case BufferSelectionStrategy.LRU -> chooseUnpinnedBufferLRUStrategy();
            case BufferSelectionStrategy.RING_BUFFER -> chooseUnpinnedBufferRingStrategy();
            default -> chooseUnpinnedBufferNaiveStrategy();
        };
    }

    private Attempt<Buffer> chooseUnpinnedBufferNaiveStrategy() {
        for (Buffer buffer : bufferPool) {
            if (!buffer.isPinned())
                return Attempt.succeeded(buffer);
        }

        // Keeping here for experimentation. If we confirmed there is an unpinned buffer, then being unable to pin
        // is a serious error, but the book doesn't account for this. I'll keep this around in case I want to change it
        // throw new RuntimeException("We thought there was an unpinned buffer to use, but somehow, we are wrong!");

        return Attempt.failed();
    }

    // Choose the buffer with the oldest pin time (lowest timestamp on last pinned time)
    private Attempt<Buffer> chooseUnpinnedBufferFIFOStrategy() {
        Optional<Buffer> fifoBuffer =  bufferPool.stream()
                .filter(buffer -> !buffer.isPinned())
                .min(Comparator.comparing(Buffer::getLastTimePinnedNano));

        return fifoBuffer.map(Attempt::succeeded).orElseGet(Attempt::failed);
    }

    // Choose the buffer with the oldest UNpinned time (lowest timestamp on last unpinned time)
    private Attempt<Buffer> chooseUnpinnedBufferLRUStrategy() {
        Optional<Buffer> lruBuffer = bufferPool.stream()
                .filter(buffer -> !buffer.isPinned())
                .min(Comparator.comparing(Buffer::getLastTimeUnpinnedNano));

        return lruBuffer.map(Attempt::succeeded).orElseGet(Attempt::failed);
    }

    private Attempt<Buffer> chooseLowestLSNStrategy() {
        Optional<Buffer> lruBuffer = bufferPool.stream()
                .filter(buffer -> !buffer.isPinned())
                .min(Comparator.comparing(Buffer::getModifyingTxNum));

        return lruBuffer.map(Attempt::succeeded).orElseGet(Attempt::failed);
    }

    private Attempt<Buffer> chooseUnpinnedBufferRingStrategy() {
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

        while(currBuffer.isPinned()) {
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

    private boolean hasWaitedTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME_WAIT_FOR_PIN_MILLISECONDS;
    }
}
