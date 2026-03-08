package transaction.concurrency;

import error.CheckpointInterruptedException;
import log.LogMgr;
import lombok.Setter;
import server.ConfigFetcher;
import transaction.recovery.recordtype.CheckpointRecord;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class QCheckpointStrategy implements CheckpointStrategy {
    private final long CHECKPOINT_WAIT_POLL_STEP_MILLIS = ConfigFetcher.getCheckpointingWaitPollStepTime();

    private final Map<Long, Boolean> activeTransactions;
    private final AtomicBoolean isPendingCheckpoint;

    @Setter
    private LogMgr logMgr;


    // The lock coordinates two things:
    //   1. Blocks new transactions from starting while a Q checkpoint is pending
    //      * Quiescent checkpoints have to stop the world in order to checkpoint
    //   2. Wakes up the checkpoint thread when active transactions drain
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition allTransactionsDrained = lock.newCondition();

    public QCheckpointStrategy(LogMgr logMgr, Map<Long, Boolean> activeTransactions, AtomicBoolean isPendingCheckpoint) {
        this.activeTransactions = activeTransactions;
        this.isPendingCheckpoint = isPendingCheckpoint;
        this.logMgr = logMgr;
    }

    /**
     * If a checkpoint is pending, block any new transactions from registering so we can get all the transactions
     * to drain. Once the checkpiont finishes, signal the Condition to unblock waiting transactions
     * @param txNum Transaction number of the started transaction
     */
    @Override
    public void doRegister(long txNum) {
        lock.lock();
        try {
            while (isPendingCheckpoint.get())
                allTransactionsDrained.await(CHECKPOINT_WAIT_POLL_STEP_MILLIS, TimeUnit.MILLISECONDS);

            activeTransactions.put(txNum, true);
        }
        catch (InterruptedException e) {
            throw new CheckpointInterruptedException("While blocking new transactions from starting, got interrupted! -> " + e);
        }
        finally {
            // Always need to unlock. If we forget or fail to, we can run into a deadlock
            lock.unlock();
        }
    }

    /**
     * Signal the checkpoint thread that a transaction finished since it may be waiting for all the active transactions
     * to drain
     * @param txNum Transaction number of the completed transaction
     */
    @Override
    public void doDeregister(long txNum) {
        lock.lock();
        try {
            activeTransactions.remove(txNum);

            if (activeTransactions.isEmpty())
                allTransactionsDrained.signalAll();
        } finally {
            // Always need to unlock. If we forget or fail to, we can run into a deadlock
            lock.unlock();
        }
    }

    /**
     * For quiescent checkpointing, we need to wait for all transactions to complete (and flush) before we can actually
     * write the checkpoint record (hence why this is such an expensive operation). All new transactions are blocked
     * until the active transactions drain and the checkpoint record is written to disk
     * @param completedTxNum
     */
    @Override
    public void checkpoint(long completedTxNum) {
        lock.lock();

        // Considering that there are multiple threads pounding this singleton, need a null check for the logMgr in
        // case transactions start before the log manager can be set. Not very likely it would happen, but better safe
        // than sorry
        if (logMgr == null)
            throw new NullPointerException("The LogMgr is null! It hasn't been set yet!");

        try {
            while(!activeTransactions.isEmpty())
                allTransactionsDrained.await(CHECKPOINT_WAIT_POLL_STEP_MILLIS, TimeUnit.MILLISECONDS);

            long lsn = CheckpointRecord.writeToLog(logMgr, completedTxNum);
            logMgr.flush(lsn);

            isPendingCheckpoint.set(false);
            allTransactionsDrained.signalAll();
        }
        catch (InterruptedException e) {
            throw new CheckpointInterruptedException("Interrupted while waiting for running transactions to drain! -> " + e);
        }
        finally {
            // Always need to unlock. If we forget or fail to, we can run into a deadlock
            lock.unlock();
        }
    }
}
