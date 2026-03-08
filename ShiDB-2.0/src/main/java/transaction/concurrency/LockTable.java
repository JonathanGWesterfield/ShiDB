package transaction.concurrency;

import error.LockAbortException;
import file.BlockId;
import server.ConfigFetcher;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Basic lock protocol:
 * 1. Before reading any block, acquire a shared lock on it
 * 2. Before writing to any block, acquire an exclusive lock on it
 * 3. Release all locks after a commit or rollback
 */

public class LockTable {
    // These need to be in nanoseconds since the system can actually run faster than the millisecond granulariyt
    // allows. However, the user will only interact with milliseconds in the config
    private static final long MAX_WAIT_TIME_NANO = ConfigFetcher.getConcurrencyMgrMaxWaitTime() * 1000;
    private static final long POLL_STEP_NANO = ConfigFetcher.getConcurrencyMgrPollStepTime() * 1000;

    private Map<BlockId, Integer> sharedLocks = new HashMap<>();
    private Set<BlockId> exclusiveLocks = new HashSet<>();

    public synchronized void setSharedLock(BlockId block) {
        try {
            long startTimeNano = System.nanoTime();
            while(!hasExclusiveLock(block) && canWaitLonger(startTimeNano)) {
                wait(POLL_STEP_NANO / 1000); // Reconvert it to milliseconds

                if (!hasExclusiveLock(block)) {
                    int sLockVal = getSharedLocksValue(block);
                    sharedLocks.put(block, sLockVal + 1);
                    return;
                }
            }

        }
        catch (InterruptedException e) {
            throw new LockAbortException("Was interrupted while trying to obtain a shared Lock for block: " + block +
                    " -> " + e);
        }

        // If we are here, then we were never able to get a shared lock
        String err = String.format("After waiting for %s milliseconds for acquiring shared lock on block: %s, timed out!",
                MAX_WAIT_TIME_NANO / 1000, block);
        throw new LockAbortException(err);
    }

    public synchronized void setExclusiveLock(BlockId block) {
        try {
            long startTimeNano = System.nanoTime();
            while (canWaitLonger(startTimeNano) && (hasOtherSharedLocks(block) || hasExclusiveLock(block))) {
                if (!hasOtherSharedLocks(block) && !hasExclusiveLock(block))
                    exclusiveLocks.add(block);
                wait(POLL_STEP_NANO / 1000); // Reconvert it to milliseconds
            }
        }
        catch (InterruptedException e) {
            throw new LockAbortException("Was interrupted while trying to obtain an exclusive Lock for block: " + block +
                    " -> " + e);
        }

        // If we are here, then we were never able to get an exclusive lock
        String err = String.format("After waiting for %s milliseconds for acquiring exclusive lock on block: %s, timed out!",
                MAX_WAIT_TIME_NANO / 1000, block);
        throw new LockAbortException(err);
    }

    // This works under the assumption that if there is an exclusive lock on block, that means there are no shared locks either
    public synchronized void unlock(BlockId block) {
        int sharedLockVal = getSharedLocksValue(block);
        if (sharedLockVal > 1) {
            sharedLocks.put(block, sharedLockVal - 1);
        }
        else {
            // If the lock they are talking about is an exclusive lock, unlock it
            exclusiveLocks.remove(block);
            // This isn't very efficient. We want to notify only threads that are waiting on this specific block, not
            // literally all threads waiting on any block. Those threads will just check their thread again and restart
            // their wait. It's not crazy expensive but it could be better.
            notifyAll();
        }
    }

    private boolean hasExclusiveLock(BlockId block) {
        return exclusiveLocks.contains(block);
    }

    private boolean hasOtherSharedLocks(BlockId block) {
        return getSharedLocksValue(block) > 1;
    }

    private boolean canWaitLonger(long starTimeNano) {
        return System.nanoTime() - starTimeNano < MAX_WAIT_TIME_NANO;
    }

    private int getSharedLocksValue(BlockId block) {
        if (!sharedLocks.containsKey(block))
            return 0;

        return sharedLocks.get(block);
    }
}
