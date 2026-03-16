package Recovery;

import File.BlockId;
import File.Page;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import Server.ConfigFetcher;
import Server.ShiDB;
import Transaction.ShouldLog;
import Transaction.Transaction;
import Transaction.Concurrency.ConcurrencyMgr;
import Transaction.Concurrency.TransactionRegistrySingleton;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for undo-redo recovery strategy.
 * Requires the config to have UNDO_REDO set as the recovery strategy.
 *
 * The key behavioral difference from undo-only: a forward redo pass replays
 * all committed transactions before the backward undo pass cleans up incomplete ones.
 * This means committed data is guaranteed to be present even if it was evicted
 * from the buffer pool before the crash (steal policy).
 */
public class RecoveryMgrUndoRedoTest extends RecoveryMgrTestBase {

    @BeforeAll
    static void loadConfig() {
        ConcurrencyMgr.reinit();
        TransactionRegistrySingleton.reinit();
        ConfigFetcher.reloadConfig("src/test/resources/undoRedoRecoveryTestConfig.json");
    }

    /**
     * Committed transactions must survive crash.
     * The redo pass guarantees their data is replayed even if it was evicted.
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

        // Simulate crash — inFlightTx abandoned
        ConcurrencyMgr.reinit(); // clear abandoned locks before recovery
        ShiDB recoveredDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);

        Transaction verifyTx = new Transaction(recoveredDB.getFileMgr(), recoveredDB.getLogMgr(), recoveredDB.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(committedValue, actual, "Committed value should survive crash under UNDO_REDO strategy");
    }

    /**
     * Uncommitted transaction must be undone in the backward pass.
     */
    @Test
    @DisplayName("Uncommitted transaction is undone in backward pass")
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
        // No commit

        // Simulate crash — inFlightTx abandoned
        ConcurrencyMgr.reinit(); // clear abandoned locks before recovery
        ShiDB recoveredDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);

        Transaction verifyTx = new Transaction(recoveredDB.getFileMgr(), recoveredDB.getLogMgr(), recoveredDB.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(originalValue, actual, "Uncommitted value should be undone in backward pass");
    }

    /**
     * The key UNDO_REDO advantage: committed data is replayed by the redo pass
     * even if the buffer pool evicted it before the crash (steal policy in action).
     * Forces eviction by filling the buffer pool with other blocks after committing.
     */
    @Test
    @DisplayName("Redo pass replays committed data evicted from buffer pool before crash")
    void redoPassReplaysEvictedCommittedData() throws IOException {
        int committedValue = 77;

        ShiDB precrashDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(precrashDB);

        Transaction tx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        tx.pin(blk);
        tx.setInt(blk, OFFSET, committedValue, ShouldLog.OK_TO_LOG);
        tx.commit();

        // Force eviction of the committed page by filling the buffer pool
        for (int i = 1; i < BUFFER_SIZE; i++) {
            BlockId extraBlk = new BlockId(TEST_FILE, i);
            Page p = new Page(BLOCK_SIZE);
            precrashDB.getFileMgr().writePageToDisk(extraBlk, p);
            precrashDB.getBufferMgr().pinBuffer(extraBlk);
        }
        // Crash without unpinning

        // Simulate crash — inFlightTx abandoned
        ConcurrencyMgr.reinit(); // clear abandoned locks before recovery
        ShiDB recoveredDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);

        Transaction verifyTx = new Transaction(recoveredDB.getFileMgr(), recoveredDB.getLogMgr(), recoveredDB.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(committedValue, actual, "Redo pass should replay committed data even if evicted before crash");
    }

    /**
     * Multiple interleaved transactions. Committed ones are redone, incomplete one is undone.
     * Verifies both passes work correctly together.
     */
    @Test
    @DisplayName("Multiple interleaved transactions — redo committed, undo incomplete")
    void multipleInterleavedTransactions_redoAndUndo() throws IOException {
        int committed1 = 11;
        int committed2 = 22;
        int uncommitted = 333;

        ShiDB precrashDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(precrashDB);

        Transaction tx1 = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        tx1.pin(blk);
        tx1.setInt(blk, OFFSET, committed1, ShouldLog.OK_TO_LOG);
        tx1.commit();

        Transaction tx2 = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        tx2.pin(blk);
        tx2.setInt(blk, OFFSET_2, committed2, ShouldLog.OK_TO_LOG);
        tx2.commit();

        Transaction crashTx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        crashTx.pin(blk);
        crashTx.setInt(blk, OFFSET, uncommitted, ShouldLog.OK_TO_LOG);
        // No commit

        // Simulate crash — inFlightTx abandoned
        ConcurrencyMgr.reinit(); // clear abandoned locks before recovery
        ShiDB recoveredDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);

        Transaction verifyTx = new Transaction(recoveredDB.getFileMgr(), recoveredDB.getLogMgr(), recoveredDB.getBufferMgr());
        verifyTx.pin(blk);
        int actual1 = verifyTx.getInt(blk, OFFSET);
        int actual2 = verifyTx.getInt(blk, OFFSET_2);
        verifyTx.commit();

        assertEquals(committed1, actual1, "Offset should be restored to committed value after undo of crashed tx");
        assertEquals(committed2, actual2, "Offset2 should retain its committed value");
    }

    /**
     * A rolled-back transaction should not interfere with recovery.
     * Its undo was already applied before the crash.
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

        // Simulate crash — inFlightTx abandoned
        ConcurrencyMgr.reinit(); // clear abandoned locks before recovery
        ShiDB recoveredDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);

        Transaction verifyTx = new Transaction(recoveredDB.getFileMgr(), recoveredDB.getLogMgr(), recoveredDB.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(originalValue, actual, "Rolled back value should not be present — rollback was already applied");
    }

    /**
     * With NQ checkpointing, transactions in-flight at checkpoint time that never
     * committed must be undone. The redo pass replays their operations first,
     * then the undo pass reverses them.
     */
    @Test
    @DisplayName("NQ CHECKPOINT: In-flight transaction at checkpoint time is redone then undone")
    void nqCheckpoint_inFlightTransactionIsRedoThenUndone() throws IOException {
        int originalValue = 1;
        int uncommittedValue = 42;

        ShiDB precrashDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(precrashDB);

        Transaction setupTx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        setupTx.pin(blk);
        setupTx.setInt(blk, OFFSET, originalValue, ShouldLog.OK_TO_LOG);
        setupTx.commit();

        Transaction inFlightTx = new Transaction(precrashDB.getFileMgr(), precrashDB.getLogMgr(), precrashDB.getBufferMgr());
        inFlightTx.pin(blk);
        inFlightTx.setInt(blk, OFFSET, uncommittedValue, ShouldLog.OK_TO_LOG);
        // NQ checkpoint fires via threshold — inFlightTx is in the running list
        // inFlightTx never commits — crash

        // Simulate crash — inFlightTx abandoned
        ConcurrencyMgr.reinit(); // clear abandoned locks before recovery
        ShiDB recoveredDB = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);

        Transaction verifyTx = new Transaction(recoveredDB.getFileMgr(), recoveredDB.getLogMgr(), recoveredDB.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(originalValue, actual, "In-flight tx at NQ checkpoint should be redone then undone — net effect is original value");
    }
}

