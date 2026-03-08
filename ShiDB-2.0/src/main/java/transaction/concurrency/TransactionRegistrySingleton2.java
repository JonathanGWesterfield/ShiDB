package transaction.concurrency;

import log.LogMgr;
import server.ConfigFetcher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionRegistrySingleton2 {
    private final int CHECKPOINT_EVERY_N_COMMITS = ConfigFetcher.getNumTransactionsPerCheckpoint();

    private final Map<Long, Boolean> activeTransactions = new ConcurrentHashMap<>();
    private final AtomicBoolean isPendingCheckpoint = new AtomicBoolean(false);
    private final AtomicInteger txCounter = new AtomicInteger(0);

    private final CheckpointStrategy checkpointStrategy;

    private static volatile TransactionRegistrySingleton2 instance;
    private static final Object mutex = new Object();

    private LogMgr logMgr;

    private TransactionRegistrySingleton2() {
        boolean useNQCheckpointing = ConfigFetcher.useNQCheckpointing();
        logMgr = null; // Does nothing but lets us know it's there. Will be populated by the setter function

        if (useNQCheckpointing)
            this.checkpointStrategy = new NQCheckpointStrategy(logMgr, activeTransactions, isPendingCheckpoint);
        else
            this.checkpointStrategy = new QCheckpointStrategy(logMgr, activeTransactions, isPendingCheckpoint);
    }

    // Since we do initialize the LogMgr in the constructor need to make sure that this class and the underlying
    // strategy class has it too
    public void setLogMgr(LogMgr logMgr) {
        this.logMgr = logMgr;
        checkpointStrategy.setLogMgr(logMgr);
    }

    // Found this specific flavor of singleton pattern on this website:
    // https://www.digitalocean.com/community/tutorials/thread-safety-in-java-singleton-classes
    public static TransactionRegistrySingleton2 getInstance() {
        TransactionRegistrySingleton2 result = instance;
        if (result == null) {
            synchronized (mutex) {
                result = instance;
                if (result == null)
                    instance = result = new TransactionRegistrySingleton2();
            }
        }
        return result;
    }

    public void registerTx(long txNum) {
        checkpointStrategy.doRegister(txNum);
    }

    public void deRegisterTx(long txNum) {
        checkpointStrategy.doDeregister(txNum);

        int txCount = txCounter.incrementAndGet();
        if (txCount >= CHECKPOINT_EVERY_N_COMMITS && isNotPendingCheckpointAndSet()) {
            txCounter.set(0);
            checkpointStrategy.checkpoint(txNum);
        }
    }

    /* AtomicBoolean.compareAndSet is hard for me understand, so I separated into a function to help readability for me.
     * AtomicBoolean.compareAndSet(false, true) does three things atomically as a single uninterruptible CPU operation:
     * 1. Read the current value of isPendingCheckpoint
     * 2. Compare it against the expected value (false)
     * 3. Only if they match, set it to the new value (true) and return true. If they don't match, do nothing and return false
     *
     * We need this atomicity in case multiple threads hit deRegister() at the same time
    */
    private boolean isNotPendingCheckpointAndSet() {
        return isPendingCheckpoint.compareAndSet(false, true);
    }
}
