package transaction.concurrency;

import error.CheckpointInterruptedException;
import log.LogMgr;
import lombok.NonNull;
import lombok.Setter;
import server.ConfigFetcher;
import transaction.recovery.recordtype.CheckpointRecord;
import transaction.recovery.recordtype.NQCheckpointRecord;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// I implemented this singleton so I can keep from cluttering the Transaction class. I need a single global state
// and a singleton gives me that and enough space to make it performant since we know many threads will be calling it.
public class TransactionRegistrySingleton {
    private final int CHECKPOINT_EVERY_N_COMMITS = ConfigFetcher.getNumTransactionsPerCheckpoint();
    private final boolean USE_NQ_CHECKPOINTS = ConfigFetcher.useNQCheckpointing();
    private final long CHECKPOINT_WAIT_POLL_STEP_MILLIS = ConfigFetcher.getCheckpointingWaitPollStepTime();

    // I wish there was a ConcurrentHashSet class, but since there isn't, we use this instead
    private Map<Long, Boolean> activeTransactions;

    private final ReentrantReadWriteLock registeringRWLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = registeringRWLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = registeringRWLock.writeLock();

    private final ReentrantLock qCheckpointLock = new ReentrantLock();
    private final Condition checkpointDone = qCheckpointLock.newCondition();

    private AtomicInteger txCounter = new AtomicInteger(0);
    private AtomicBoolean isPendingCheckpoint = new AtomicBoolean(false);

    private static volatile TransactionRegistrySingleton instance;

    @Setter @NonNull
    private LogMgr logMgr;

    private static Object mutex = new Object();

    private TransactionRegistrySingleton() {
        this.activeTransactions = new ConcurrentHashMap<>();
    }

    // Found this pattern on this website:
    // https://www.digitalocean.com/community/tutorials/thread-safety-in-java-singleton-classes
    public static TransactionRegistrySingleton getInstance() {
        TransactionRegistrySingleton result = instance;
        if (result == null) {
            synchronized (mutex) {
                result = instance;
                if (result == null)
                    instance = result = new TransactionRegistrySingleton();
            }
        }
        return result;
    }

    public void registerTx(long txNum) {
        // The readlock isn't to protect the ConcurrentHashmap backing this class, it is to support us when we need
        // to get the active Transactions (snapshot). If threads are writing to the ConcurrentHashMap at the same time,
        // the activeTransactions list we return to the user won't be accurate. We need to stop the world when returning
        // the list of active transactions

        // Read lock isn't really for reading per se, it's mostly as a "shared lock" vs the "exclusive lock" we need
        // when we fetch all the active transactions. Also, make sure no other code path acquires these in the opposite
        // order, otherwise you risk deadlock.
        readLock.lock();
        qCheckpointLock.lock();
        try {
            while(isPendingCheckpoint.get())
                checkpointDone.await();

            activeTransactions.put(txNum, true);
        }
        catch(InterruptedException e) {
            throw new CheckpointInterruptedException("While blocking new transactions from starting, got interrupted! -> " + e);
        }
        finally {
            qCheckpointLock.unlock();
            readLock.unlock();
        }
    }

    public void deRegisterTx(long txNum) {
        readLock.lock();
        try {
            activeTransactions.remove(txNum);
        } finally {
            readLock.unlock();
        }

        qCheckpointLock.lock();
        try {
            checkpointDone.signalAll();
        } finally {
            qCheckpointLock.unlock();
        }

        int count = txCounter.incrementAndGet();
        if (count >= CHECKPOINT_EVERY_N_COMMITS && isPendingCheckpoint.compareAndSet(false, true)) {
            txCounter.set(0);
            doCheckpoint(txNum);
        }
    }

    private void doCheckpoint(long completedTxNum) {
        // Considering that there are multiple threads pounding this singleton, need a null check for the logMgr in
        // case transactions start before the log manager can be set. Not very likely it would happen, but better safe
        // than sorry
        if (logMgr == null) {
            throw new NullPointerException("The LogMgr is null! It hasn't been set yet!");
        }

        if (USE_NQ_CHECKPOINTS) {
            nonQCheckpoint(completedTxNum);

        } else {
            qCheckpoint(completedTxNum);
        }
    }

    // We need to wait until all transactions have committed before we write the checkpoint log record. Need a separate
    // locking mechanism to wait until all running transactions have committed. Locking mechanism also needs
    // to ensure that no other transactions are allowed to start until the checkpoint has been written
    private void qCheckpoint(long completedTxNum) {
        qCheckpointLock.lock();
        try {
            while (!activeTransactions.isEmpty())
                checkpointDone.await(CHECKPOINT_WAIT_POLL_STEP_MILLIS, TimeUnit.MILLISECONDS);

            long lsn = CheckpointRecord.writeToLog(logMgr, completedTxNum);
            logMgr.flush(lsn);

            isPendingCheckpoint.set(false);
            checkpointDone.signalAll();
            txCounter.set(0);
        }
        catch(InterruptedException e) {
            throw new CheckpointInterruptedException("While waiting for running transaction to drain, got interrupted! -> " + e);
        }
        finally {
            qCheckpointLock.unlock();
        }
    }

    private void nonQCheckpoint(long completedTxNum) {
        writeLock.lock();
        try {
            ArrayList<Long> currActiveTxs = new ArrayList<>(activeTransactions.keySet());
            NQCheckpointRecord.writeToLog(logMgr, completedTxNum, currActiveTxs);
            txCounter.set(0);
        } finally {
            writeLock.unlock();
        }
    }
}
