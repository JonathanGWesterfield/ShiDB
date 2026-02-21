package buffer;

import error.BufferAbortException;
import file.BlockId;
import file.FileMgr;
import file.Size;
import server.ConfigFetcher;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class BufferMgr {

    protected int numTotalBuffers;
    protected AtomicInteger numAvailableBuffers;
    protected FileMgr fileMgr;

    private static final long MAX_TIME_WAIT_FOR_PIN_MILLISECONDS = ConfigFetcher.getBufferMgrMaxWaitTime();
    private static final long WAIT_TIME_STEP_MILLISECONDS = ConfigFetcher.getBufferMgrPollStepTime();

    // No arg constructors are a weird archaic java thing
    public BufferMgr() {}

    public long getTotalSpaceOfBufferPool(Size size) {
        int numBytes = numTotalBuffers * fileMgr.getBlocksize();

        switch (size) {
            case Size.MEGABYTES -> { return numBytes / 1000; }
            case Size.GIGABYTES -> { return numBytes / 1000000; }
            default -> { return numBytes; }
        }
    }

    public long getCurrentlyUsedBufferSpace(Size size) {
        int numBytes = (numTotalBuffers - numAvailableBuffers.intValue()) * fileMgr.getBlocksize();

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

    public abstract void unpinBuffer(Buffer buffer);

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

    protected Attempt<Buffer> tryToPin(BlockId block) {
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

    private boolean hasWaitedTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME_WAIT_FOR_PIN_MILLISECONDS;
    }

    protected abstract Attempt<Buffer> findExistingBuffer(BlockId block);

    protected abstract Attempt<Buffer> chooseUnPinnedBuffer();
}
