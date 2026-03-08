package transaction.concurrency;

import log.LogMgr;
import lombok.Setter;
import transaction.recovery.recordtype.NQCheckpointRecord;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NQCheckpointStrategy implements CheckpointStrategy {
    private final Map<Long, Boolean> activeTransactions;
    private final AtomicBoolean isPendingCheckpoint;

    @Setter
    private LogMgr logMgr;


    // Read lock: allows concurrent registrations and deregistrations
    // Write lock: taken exclusively during snapshot to freeze the active transaction set
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    public NQCheckpointStrategy(LogMgr logMgr, Map<Long, Boolean> activeTransactions, AtomicBoolean isPendingCheckpoint) {
        this.activeTransactions = activeTransactions;
        this.isPendingCheckpoint = isPendingCheckpoint;
        this.logMgr = logMgr;
    }

    /**
     * Acquire lock so that when we try to get a snapshot of all running transactions during the checkpoint step, some
     * thread doesn't swoop in an add another transaction, invalidating our running Tx list. Since this is
     * Non-Quiescent checkpointing, we only need to halt new transactions when fetching the list of running transactions,
     * as opposed to quiescent checkpointing where we need to halt until ALL transactions complete
     * @param txNum Transaction number of the started transaction
     */
    @Override
    public void doRegister(long txNum) {
        readLock.lock();
        try {
            activeTransactions.put(txNum, true);
        }
        finally {
            // Always need to unlock. If we forget or fail to, we can run into a deadlock
            readLock.unlock();
        }
    }

    /**
     * Similar concept to the doRegister function where we need a snapshot of the running transactions, so we need to
     * halt so we can give an accurate accounting for the NQ checkpoint to work
     * @param txNum Transaction number of the completed transaction
     */
    @Override
    public void doDeregister(long txNum) {
        readLock.lock();
        try {
            activeTransactions.remove(txNum);
        } finally {
            // Always need to unlock. If we forget or fail to, we can run into a deadlock
            readLock.unlock();
        }
    }

    /**
     * Lock up the list of active transactions so we can get an accurate snapshot, then write to an NQ checkpoint record.
     * @param completedTxNum Transaction number of the completed transaction that triggered the checkpoint
     */
    @Override
    public void checkpoint(long completedTxNum) {
        writeLock.lock();

        // Considering that there are multiple threads pounding this singleton, need a null check for the logMgr in
        // case transactions start before the log manager can be set. Not very likely it would happen, but better safe
        // than sorry
        if (logMgr == null)
            throw new NullPointerException("The LogMgr is null! It hasn't been set yet!");

        try {
            ArrayList<Long> currActiveTxs = new ArrayList<>(activeTransactions.keySet());
            NQCheckpointRecord.writeToLog(logMgr, completedTxNum, currActiveTxs);
            isPendingCheckpoint.set(false);
        }
        finally {
            // Always need to unlock. If we forget or fail to, we can run into a deadlock
            writeLock.unlock();
        }
    }
}
