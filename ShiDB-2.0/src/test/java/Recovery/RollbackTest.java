package Recovery;

import File.BlockId;
import File.Page;
import org.junit.jupiter.api.*;
import Server.ShiDB;
import Transaction.ShouldLog;
import Transaction.Transaction;
import Transaction.Concurrency.ConcurrencyMgr;
import Transaction.Concurrency.TransactionRegistrySingleton;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static Server.ConfigFetcher.getConfigs;

/**
 * Unit tests for RecoveryMgr.doRollback() exercised through Transaction.rollback().
 *
 * Tests verify:
 *  - rollback undoes the transaction's own writes
 *  - rollback stops at the transaction's own START record (doesn't undo earlier transactions)
 *  - rollback skips records belonging to other transactions
 *  - rollback does not double-undo an already rolled-back transaction
 *  - rollback correctly undoes multiple writes within the same transaction in reverse order
 */
public class RollbackTest extends RecoveryMgrTestBase {

    @BeforeEach
    void setUp() throws IOException {
        ConcurrencyMgr.reinit();
        TransactionRegistrySingleton.reinit();
        getConfigs("src/test/resources/undoOnlyRecoveryTestConfig.json");
        deleteDirectory(TEST_DIR);
    }

    @AfterEach
    void cleanUp() throws IOException {
        deleteDirectory(TEST_DIR);
    }

    private void deleteDirectory(String dirName) throws IOException {
        Path path = Path.of(dirName);
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    // -------------------------------------------------------------------------
    // Basic rollback
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Rollback restores the pre-transaction value")
    void rollback_restoresPreTransactionValue() throws IOException {
        int originalValue = 10;
        int rolledBackValue = 999;

        ShiDB db = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(db);

        // Write the original value and commit
        Transaction setupTx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        setupTx.pin(blk);
        setupTx.setInt(blk, OFFSET, originalValue, ShouldLog.OK_TO_LOG);
        setupTx.commit();

        // Write a new value then roll back
        Transaction tx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        tx.pin(blk);
        tx.setInt(blk, OFFSET, rolledBackValue, ShouldLog.OK_TO_LOG);
        tx.rollback();

        // Verify the value was restored
        ConcurrencyMgr.reinit();
        Transaction verifyTx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(originalValue, actual,
                "Rollback should restore the value to what it was before the transaction");
    }

    // -------------------------------------------------------------------------
    // Stops at START — does not undo earlier transactions
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Rollback stops at own START record — does not undo earlier committed transactions")
    void rollback_stopsAtOwnStartRecord() throws IOException {
        int committedValue = 42;
        int rolledBackValue = 999;

        ShiDB db = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(db);

        // Commit a value from an earlier transaction
        Transaction committedTx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        committedTx.pin(blk);
        committedTx.setInt(blk, OFFSET, committedValue, ShouldLog.OK_TO_LOG);
        committedTx.commit();

        // Roll back a later transaction
        Transaction rolledBackTx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        rolledBackTx.pin(blk);
        rolledBackTx.setInt(blk, OFFSET, rolledBackValue, ShouldLog.OK_TO_LOG);
        rolledBackTx.rollback();

        // The committed value should be restored — rollback must not have crossed
        // the START record and undone the earlier committed transaction
        ConcurrencyMgr.reinit();
        Transaction verifyTx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(committedValue, actual,
                "Rollback must stop at own START — earlier committed value should be intact");
    }

    // -------------------------------------------------------------------------
    // Skips other transactions' records
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Rollback skips records from other transactions — only undoes own writes")
    void rollback_skipsOtherTransactionRecords() throws IOException {
        int originalValue = 5;
        int otherTxValue = 50;
        int rolledBackValue = 999;

        ShiDB db = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(db);
        BlockId blk2 = new BlockId(TEST_FILE, 1);

        // Write initial page for blk2
        Page page = new Page(db.getFileMgr().getBlocksize());
        page.setInt(OFFSET, 0);
        db.getFileMgr().writePageToDisk(blk2, page);

        // Setup: write originalValue to blk
        Transaction setupTx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        setupTx.pin(blk);
        setupTx.setInt(blk, OFFSET, originalValue, ShouldLog.OK_TO_LOG);
        setupTx.commit();

        // otherTx writes to blk2 and commits — its records are interleaved in the log
        Transaction otherTx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        otherTx.pin(blk2);
        otherTx.setInt(blk2, OFFSET, otherTxValue, ShouldLog.OK_TO_LOG);
        otherTx.commit();

        // Our transaction writes to blk and rolls back
        Transaction rolledBackTx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        rolledBackTx.pin(blk);
        rolledBackTx.setInt(blk, OFFSET, rolledBackValue, ShouldLog.OK_TO_LOG);
        rolledBackTx.rollback();

        ConcurrencyMgr.reinit();

        // blk should be restored to originalValue
        Transaction verifyTx1 = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        verifyTx1.pin(blk);
        int actualBlk = verifyTx1.getInt(blk, OFFSET);
        verifyTx1.commit();

        ConcurrencyMgr.reinit();

        // blk2 should still have otherTxValue — rollback must not have touched it
        Transaction verifyTx2 = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        verifyTx2.pin(blk2);
        int actualBlk2 = verifyTx2.getInt(blk2, OFFSET);
        verifyTx2.commit();

        assertEquals(originalValue, actualBlk,
                "Rolled-back transaction's write should be undone");
        assertEquals(otherTxValue, actualBlk2,
                "Other committed transaction's write should not be affected by rollback");
    }

    // -------------------------------------------------------------------------
    // Does not double-undo
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("A rolled-back transaction is not double-undone on a second rollback attempt")
    void rollback_doesNotDoubleUndo() throws IOException {
        int originalValue = 7;
        int rolledBackValue = 777;

        ShiDB db = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(db);

        Transaction setupTx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        setupTx.pin(blk);
        setupTx.setInt(blk, OFFSET, originalValue, ShouldLog.OK_TO_LOG);
        setupTx.commit();

        // First transaction: write and roll back
        Transaction tx1 = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        tx1.pin(blk);
        tx1.setInt(blk, OFFSET, rolledBackValue, ShouldLog.OK_TO_LOG);
        tx1.rollback();

        ConcurrencyMgr.reinit();

        // Second transaction: write a new value and also roll back
        // If doRollback incorrectly scans past the ROLLBACK record of tx1 and re-undoes it,
        // the value will be wrong
        int tx2Value = 888;
        Transaction tx2 = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        tx2.pin(blk);
        tx2.setInt(blk, OFFSET, tx2Value, ShouldLog.OK_TO_LOG);
        tx2.rollback();

        ConcurrencyMgr.reinit();

        Transaction verifyTx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        verifyTx.pin(blk);
        int actual = verifyTx.getInt(blk, OFFSET);
        verifyTx.commit();

        assertEquals(originalValue, actual,
                "Value should be originalValue — tx2 rollback should undo only tx2's write, not re-undo tx1");
    }

    // -------------------------------------------------------------------------
    // Multiple writes rolled back in reverse order
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Rollback undoes multiple writes within the same transaction in reverse order")
    void rollback_undoesMultipleWritesInReverseOrder() throws IOException {
        int originalValue1 = 1;
        int originalValue2 = 2;
        int write1 = 100;
        int write2 = 200;
        int write3 = 300; // third write to OFFSET overwrites write1 and write2's effects on OFFSET

        ShiDB db = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
        BlockId blk = initializeBlock(db);

        // Setup two offsets with known original values
        Transaction setupTx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        setupTx.pin(blk);
        setupTx.setInt(blk, OFFSET, originalValue1, ShouldLog.OK_TO_LOG);
        setupTx.setInt(blk, OFFSET_2, originalValue2, ShouldLog.OK_TO_LOG);
        setupTx.commit();

        // Transaction writes to both offsets multiple times then rolls back
        Transaction tx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        tx.pin(blk);
        tx.setInt(blk, OFFSET, write1, ShouldLog.OK_TO_LOG);   // first write to OFFSET
        tx.setInt(blk, OFFSET_2, write2, ShouldLog.OK_TO_LOG); // write to OFFSET_2
        tx.setInt(blk, OFFSET, write3, ShouldLog.OK_TO_LOG);   // second write to OFFSET
        tx.rollback();

        ConcurrencyMgr.reinit();

        Transaction verifyTx = new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
        verifyTx.pin(blk);
        int actualOffset = verifyTx.getInt(blk, OFFSET);
        int actualOffset2 = verifyTx.getInt(blk, OFFSET_2);
        verifyTx.commit();

        assertEquals(originalValue1, actualOffset,
                "OFFSET should be restored to originalValue1 after rolling back all writes");
        assertEquals(originalValue2, actualOffset2,
                "OFFSET_2 should be restored to originalValue2 after rolling back its write");
    }
}
