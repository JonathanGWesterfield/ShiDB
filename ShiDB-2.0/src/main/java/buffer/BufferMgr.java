package buffer;

import error.BufferAbortException;
import file.BlockId;
import file.FileMgr;
import log.LogMgr;
import server.ConfigFetcher;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferMgr {

    enum bufferSelectionStrategy {
        NAIVE, // Choose the first unpinned buffer it finds
        FIFO, // Choose the unpinned buffer whose contents were unpinned least recently (pinned -> lowest sys time)
        LRU, // Choose the unpinned buffer whose contents were unpinned least recently (unpinned -> lowest syst time)
        RING_BUFFER // Scan buffers sequentially  from last replaced buffer; choose first unpinned buffer
    }

    // TODO: Create an pinned and unpinned buffer pool. Unpinned needs to be a blocking priority queue to support all
    //  of the different buffer selection strategies
    private ArrayList<Buffer> bufferPool;

    private AtomicInteger numAvailableBuffers;
    private static final long MAX_TIME_WAIT_FOR_PIN_MILLISECONDS = ConfigFetcher.getBufferMgrMaxWaitTime();
    private static final long WAIT_TIME_STEP_MILLISECONDS = ConfigFetcher.getBufferMgrPollStepTime();

    public BufferMgr(FileMgr fileMgr, LogMgr logMgr, int numBuffers) {
        bufferPool = new ArrayList<>();
        numAvailableBuffers = new AtomicInteger(numBuffers);

        for (int i = 0; i < numBuffers; i++)
            bufferPool.add(new Buffer(fileMgr, logMgr));
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

        Buffer buffer = attemptFindExisting.value();
        if (attemptFindExisting.hasFailed()) {
            Attempt<Buffer> attemptChooseUnpinnedBuffer = chooseUnPinnedBuffer();
            if (attemptChooseUnpinnedBuffer.hasFailed())
                return Attempt.failed();

            buffer = attemptChooseUnpinnedBuffer.value();
            buffer.assignToBlock(block);
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
        for (Buffer buffer : bufferPool) {
            if (!buffer.isPinned())
                return Attempt.succeeded(buffer);
        }

        // Keeping here for experimentation. If we confirmed there is an unpinned buffer, then being unable to pin
        // is a serious error, but the book doesn't account for this. I'll keep this around in case I want to change it
        // throw new RuntimeException("We thought there was an unpinned buffer to use, but somehow, we are wrong!");

        return Attempt.failed();
    }

    private boolean hasWaitedTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME_WAIT_FOR_PIN_MILLISECONDS;
    }


}
