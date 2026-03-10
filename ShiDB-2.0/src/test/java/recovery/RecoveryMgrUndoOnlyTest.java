package recovery;

import file.BlockId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import server.ConfigFetcher;
import server.ShiDB;
import transaction.ShouldLog;
import transaction.Transaction;
import transaction.concurrency.ConcurrencyMgr;
import transaction.concurrency.TransactionRegistrySingleton;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for undo-only recovery strategy.
 * Requires the config to have UNDO_ONLY set as the recovery strategy.
 */
public class RecoveryMgrUndoOnlyTest extends RecoveryMgrTestBase {

    @BeforeEach
    void Setup() {
        ConcurrencyMgr.reinit();
        TransactionRegistrySingleton.reinit();
        ConfigFetcher.reloadConfig("src/test/resources/undoOnlyRecoveryTestConfig.json");
    }

    /**
     * Committed transactions must survive a crash.
     * Undo-only has no redo pass, so durability relies entirely on the buffer
     * manager having flushed committed pages before the crash.
     */
    @Test
    @DisplayName("Committed transaction data survives crash")
    void committedTransactionSurvivesCrash() throws IOException {
        int committedValue = 42;

        ShiDB precrashDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(precrashDB);

        Transaction tx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        tx.pin(blk);
        tx.setInt(blk, OFFSET, committedValue, ShouldLog.OK_TO_LOG);
        tx.commit();

        ShiDB recoveredDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);

        Transaction verifyTx = new Transaction(recoveredDB.getFileMgr(), recoveredDB.getLogMgr(), recoveredDB.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(committedValue, actual, "Committed value should survive crash and recovery");
    }

    /**
     * An uncommitted transaction must be undone during recovery.
     * The value it wrote should be replaced with the pre-transaction value.
     */
    @Test
    @DisplayName("Uncommitted transaction is rolled back after crash")
    void uncommittedTransactionIsUndone() throws IOException {
        int originalValue = 10;
        int uncommittedValue = 999;

        ShiDB precrashDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(precrashDB);

        Transaction setupTx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        setupTx.pin(blk);
        setupTx.setInt(blk, OFFSET, originalValue, ShouldLog.OK_TO_LOG);
        setupTx.commit();

        Transaction crashTx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        crashTx.pin(blk);
        crashTx.setInt(blk, OFFSET, uncommittedValue, ShouldLog.OK_TO_LOG);
        // No commit — simulates crash

        // Simulate crash — inFlightTx abandoned
        ConcurrencyMgr.reinit(); // clear abandoned locks before recovery
        ShiDB recoveredDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);

        Transaction verifyTx = new Transaction(recoveredDB.getFileMgr(), recoveredDB.getLogMgr(), recoveredDB.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(originalValue, actual, "Uncommitted value should be undone — original value should be restored");
    }

    /**
     * When both committed and uncommitted transactions exist, only the uncommitted
     * one should be undone. The last committed value should be the final state.
     */
    @Test
    @DisplayName("Mixed transactions — only uncommitted are undone")
    void mixedTransactions_onlyUncommittedAreUndone() throws IOException {
        int originalValue = 5;
        int committedValue = 100;
        int uncommittedValue = 777;

        ShiDB precrashDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(precrashDB);

        Transaction setupTx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        setupTx.pin(blk);
        setupTx.setInt(blk, OFFSET, originalValue, ShouldLog.OK_TO_LOG);
        setupTx.commit();

        Transaction committedTx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        committedTx.pin(blk);
        committedTx.setInt(blk, OFFSET, committedValue, ShouldLog.OK_TO_LOG);
        committedTx.commit();

        Transaction crashTx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        crashTx.pin(blk);
        crashTx.setInt(blk, OFFSET, uncommittedValue, ShouldLog.OK_TO_LOG);
        // No commit

        // Simulate crash — inFlightTx abandoned
        ConcurrencyMgr.reinit(); // clear abandoned locks before recovery
        ShiDB recoveredDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);

        Transaction verifyTx = new Transaction(recoveredDB.getFileMgr(), recoveredDB.getLogMgr(), recoveredDB.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(committedValue, actual, "Should restore to last committed value after undoing uncommitted tx");
    }

    /**
     * A transaction that explicitly rolled back before the crash should not be
     * undone again during recovery. Its undo was already applied.
     */
    @Test
    @DisplayName("Explicitly rolled back transaction is not double-undone")
    void rolledBackTransactionIsNotDoubleUndone() throws IOException {
        int originalValue = 55;
        int rolledBackValue = 888;

        ShiDB precrashDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(precrashDB);

        Transaction setupTx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        setupTx.pin(blk);
        setupTx.setInt(blk, OFFSET, originalValue, ShouldLog.OK_TO_LOG);
        setupTx.commit();

        Transaction rollbackTx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        rollbackTx.pin(blk);
        rollbackTx.setInt(blk, OFFSET, rolledBackValue, ShouldLog.OK_TO_LOG);
        rollbackTx.rollback();

        ShiDB recoveredDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);

        Transaction verifyTx = new Transaction(recoveredDB.getFileMgr(), recoveredDB.getLogMgr(), recoveredDB.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(originalValue, actual, "Rolled back value should not be present — rollback was already applied");
    }

    /**
     * With NQ checkpointing, uncommitted transactions that were in-flight at
     * checkpoint time must still be undone during recovery.
     */
    @Test
    @DisplayName("NQ CHECKPOINT: In-flight transaction at checkpoint time is undone after crash")
    void nqCheckpoint_inFlightTransactionIsUndone() throws IOException {
        int originalValue = 1;
        int uncommittedValue = 42;

        ShiDB precrashDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(precrashDB);

        Transaction setupTx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        setupTx.pin(blk);
        setupTx.setInt(blk, OFFSET, originalValue, ShouldLog.OK_TO_LOG);
        setupTx.commit();

        // Start a transaction that will be in-flight when the NQ checkpoint threshold is hit
        Transaction inFlightTx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        inFlightTx.pin(blk);
        inFlightTx.setInt(blk, OFFSET, uncommittedValue, ShouldLog.OK_TO_LOG);
        // NQ checkpoint fires via deregister threshold on setupTx completing
        // inFlightTx never commits — crash

        // Simulate crash — inFlightTx abandoned
        ConcurrencyMgr.reinit(); // clear abandoned locks before recovery
        ShiDB recoveredDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);

        Transaction verifyTx = new Transaction(recoveredDB.getFileMgr(), recoveredDB.getLogMgr(), recoveredDB.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(originalValue, actual, "In-flight transaction at NQ checkpoint time should be undone");
    }
}
