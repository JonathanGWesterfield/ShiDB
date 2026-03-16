package Transaction.Concurrency;

import File.BlockId;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.TestOnly;

import java.util.HashMap;
import java.util.Map;

@Slf4j (topic = "ConcurrencyMgr")
public class ConcurrencyMgr {
    enum LockType {
        SHARED,
        EXCLUSIVE
    }

    // This effectively makes this a private singleton instance. Every instance of the ConcurrencyMgr (1 per transaction)
    // will use the same lock table. In a concurrent environment with potentially multiple threads, this is a bit shitty.
    private static LockTable lockTable = new LockTable();

    private Map<BlockId, LockType> locks = new HashMap<>();

    @TestOnly
    public static void reinit() {
        log.debug("Reiniting the ConcurrencyMgr");
        lockTable.clearLocks();
    }

    public void setSharedLock(BlockId block) {
        if (!locks.containsKey(block)) {
            lockTable.setSharedLock(block);
            locks.put(block, LockType.SHARED);
        }
    }

    private boolean hasExclusiveLock(BlockId block) {
        if (!locks.containsKey(block))
            return false;

        return locks.get(block).equals(LockType.EXCLUSIVE);
    }

    public void setExclusiveLock(BlockId block) {
        // If block already has a lock on it, do nothing
        if (!hasExclusiveLock(block)) {
            setSharedLock(block);
            lockTable.setExclusiveLock(block);
            locks.put(block, LockType.EXCLUSIVE);
        }
    }

    public void releaseAllLocks() {
        for (BlockId block : locks.keySet())
            lockTable.unlock(block);

        locks.clear();
    }
}

