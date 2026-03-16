package Transaction.Concurrency;

import Error.LockAbortException;
import File.BlockId;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.TestOnly;
import Server.ConfigFetcher;

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
@Slf4j
public class LockTable {
    private Map<BlockId, Integer> sharedLocks = new HashMap<>();
    private Set<BlockId> exclusiveLocks = new HashSet<>();

    // This function is only used for testing. Since this class is effectively a singleton, it needs to be cleared
    // between each unit test
    @TestOnly
    public synchronized void clearLocks() {
        log.debug("Clearing the LockTable");
        sharedLocks.clear();
        exclusiveLocks.clear();
        notifyAll(); // Wake any threads waiting on locks that no longer exist
    }

    public synchronized void setSharedLock(BlockId block) {
        try {
            long startTimeNano = System.nanoTime();

            log.debug("Before Wait loop -> slocks: {}, xlocks: {}", sharedLocks.toString(), exclusiveLocks.toString());
            int counter = 0;
            while (hasExclusiveLock(block)) {
                if (!canWaitLonger(startTimeNano)) {
                    String err = String.format("After waiting for %s milliseconds for acquiring shared lock on block: %s, timed out!",
                            ConfigFetcher.getConcurrencyMgrMaxWaitTimeNano() / 1_000_000L, block);
                    throw new LockAbortException(err);
                }


                log.debug("Shared lock block: {} wait loop Iter: {} -> slock: {}, xlocks: {}", block, counter, sharedLocks, exclusiveLocks);
                counter++;

                wait(ConfigFetcher.getConcurrencyMgrPollStepTimeNano() / 1_000_000);
            }
            sharedLocks.put(block, getSharedLocksValue(block) + 1);
        }
        catch (InterruptedException e) {
            throw new LockAbortException("Was interrupted while trying to obtain a shared Lock for block: " + block +
                    " -> " + e);
        }
    }

    public synchronized void setExclusiveLock(BlockId block) {
        try {
            long startTimeNano = System.nanoTime();
            while (hasOtherSharedLocks(block) || hasExclusiveLock(block)) {
                if (!canWaitLonger(startTimeNano)) {
                    String err = String.format("After waiting for %s milliseconds for acquiring exclusive lock on block: %s, timed out!",
                            ConfigFetcher.getConcurrencyMgrMaxWaitTimeNano() / 1_000_000L, block);
                    throw new LockAbortException(err);
                }
                wait(ConfigFetcher.getConcurrencyMgrPollStepTimeNano() / 1_000_000L);
            }
            exclusiveLocks.add(block);
        }
        catch (InterruptedException e) {
            throw new LockAbortException("Was interrupted while trying to obtain an exclusive Lock for block: " + block +
                    " -> " + e);
        }
    }

    // This works under the assumption that if there is an exclusive lock on block, that means there are no shared locks either
    public synchronized void unlock(BlockId block) {
        int sharedLockVal = getSharedLocksValue(block);
        if (sharedLockVal > 1) {
            sharedLocks.put(block, sharedLockVal - 1);
        }
        else {
            sharedLocks.remove(block);

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

    private boolean canWaitLonger(long startTimeNano) {
        return System.nanoTime() - startTimeNano < ConfigFetcher.getConcurrencyMgrMaxWaitTimeNano();
    }

    private int getSharedLocksValue(BlockId block) {
        if (!sharedLocks.containsKey(block))
            return 0;

        return sharedLocks.get(block);
    }
}
