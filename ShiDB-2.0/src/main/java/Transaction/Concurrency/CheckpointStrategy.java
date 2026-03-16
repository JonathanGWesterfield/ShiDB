package Transaction.Concurrency;

import Log.LogMgr;

public interface CheckpointStrategy {

    /**
     * Called when a new transaction is registering.
     * @param txNum Transaction number of the started transaction
     */
    void doRegister(long txNum);

    /**
     * Called when a transaction has completed (committed or rolled back).
     * Signals any waiting checkpoint that the active transaction count has changed
     * @param txNum Transaction number of the completed transaction
     */
    void doDeregister(long txNum);

    void checkpoint(long completedTxNum);

    void setLogMgr(LogMgr logMgr);
}
