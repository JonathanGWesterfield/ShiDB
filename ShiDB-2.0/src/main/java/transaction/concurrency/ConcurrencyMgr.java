package transaction.concurrency;

import file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyMgr {
    enum LockType {
        SHARED,
        EXCLUSIVE
    }

    // This effectively makes this a private singleton instance. Every instance of the ConcurrencyMgr (1 per transaction)
    // will use the same lock table. In a concurrent environment with potentially multiple threads, this is a bit shitty.
    private static LockTable lockTable = new LockTable();

    private Map<BlockId, LockType> locks = new HashMap<>();

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

