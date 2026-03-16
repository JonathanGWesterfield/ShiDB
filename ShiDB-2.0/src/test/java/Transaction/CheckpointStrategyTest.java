package Transaction;

import Log.LogMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import Server.ConfigFetcher;
import Server.ShiDB;
import Transaction.Concurrency.NQCheckpointStrategy;
import Transaction.Concurrency.QCheckpointStrategy;
import Transaction.Recovery.LogRecord;
import Transaction.Recovery.LogRecordFactory;
import Transaction.Recovery.RecordType.NQCheckpointRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class CheckpointStrategyTest {

    private static final String TEST_DIR = "CheckpointStrategy-unit-test";

    private ShiDB shiDB;
    private LogMgr logMgr;
    private Map<Long, Boolean> activeTransactions;
    private AtomicBoolean isPendingCheckpoint;

    @BeforeEach
    void setUp() throws IOException {
        ConfigFetcher.getConfigs("src/test/resources/undoOnlyRecoveryTestConfig.json");
        shiDB = new ShiDB(TEST_DIR, 400);
        logMgr = shiDB.getLogMgr();
        clearLogFile();

        activeTransactions = new ConcurrentHashMap<>();
        isPendingCheckpoint = new AtomicBoolean(false);
    }

    @AfterEach
    void cleanUp() throws IOException {
        deleteDirectory(TEST_DIR);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void clearLogFile() throws IOException {
        File testLogFile = new File(TEST_DIR, logMgr.getLogFile());
        if (testLogFile.exists() && testLogFile.isFile())
            testLogFile.delete();
        testLogFile.createNewFile();
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

    private NQCheckpointStrategy newNQStrategy() {
        return new NQCheckpointStrategy(logMgr, activeTransactions, isPendingCheckpoint);
    }

    private QCheckpointStrategy newQStrategy() {
        return new QCheckpointStrategy(logMgr, activeTransactions, isPendingCheckpoint);
    }

    private LogRecord findMostRecentCheckpoint() {
        Iterator<byte[]> iter = logMgr.iterator();
        while (iter.hasNext()) {
            LogRecord record = LogRecordFactory.convertToLogRecord(iter.next());
            if (record.getOperator() == LogRecord.CHECKPOINT || record.getOperator() == LogRecord.NQ_CHECKPOINT)
                return record;
        }
        return null;
    }

    // =========================================================================
    // NQCheckpointStrategy
    // =========================================================================

    // -------------------------------------------------------------------------
    // doRegister / doDeregister
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("NQ: doRegister adds transaction to active set")
    void nq_doRegister_addsToActiveTransactions() {
        NQCheckpointStrategy strategy = newNQStrategy();
        strategy.doRegister(1L);

        assertTrue(activeTransactions.containsKey(1L),
                "Registered transaction should appear in activeTransactions");
    }

    @Test
    @DisplayName("NQ: doDeregister removes transaction from active set")
    void nq_doDeregister_removesFromActiveTransactions() {
        NQCheckpointStrategy strategy = newNQStrategy();
        strategy.doRegister(1L);
        strategy.doDeregister(1L);

        assertFalse(activeTransactions.containsKey(1L),
                "Deregistered transaction should be removed from activeTransactions");
    }

    @Test
    @DisplayName("NQ: doRegister allows concurrent registrations without blocking")
    void nq_doRegister_allowsConcurrentRegistrations() throws InterruptedException {
        NQCheckpointStrategy strategy = newNQStrategy();
        int numThreads = 10;
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (long i = 1; i <= numThreads; i++) {
            long txNum = i;
            executor.submit(() -> {
                strategy.doRegister(txNum);
                doneLatch.countDown();
            });
        }

        assertTrue(doneLatch.await(2, TimeUnit.SECONDS),
                "All concurrent registrations should complete without deadlock");
        assertEquals(numThreads, activeTransactions.size(),
                "All registered transactions should be in the active set");
        executor.shutdown();
    }

    // -------------------------------------------------------------------------
    // checkpoint()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("NQ: checkpoint writes NQ_CHECKPOINT record to log")
    void nq_checkpoint_writesNQCheckpointRecord() {
        NQCheckpointStrategy strategy = newNQStrategy();
        strategy.checkpoint(1L);

        LogRecord record = findMostRecentCheckpoint();
        assertNotNull(record, "A checkpoint record should have been written");
        assertEquals(LogRecord.NQ_CHECKPOINT, record.getOperator(),
                "Record should be an NQ_CHECKPOINT");
    }

    @Test
    @DisplayName("NQ: checkpoint captures currently active transactions in snapshot")
    void nq_checkpoint_capturesActiveTxSnapshot() {
        NQCheckpointStrategy strategy = newNQStrategy();

        strategy.doRegister(10L);
        strategy.doRegister(20L);
        strategy.doRegister(30L);

        strategy.checkpoint(99L);

        NQCheckpointRecord record = (NQCheckpointRecord) findMostRecentCheckpoint();
        assertNotNull(record);
        assertTrue(record.getRunningTxNums().containsAll(java.util.List.of(10L, 20L, 30L)),
                "All active transactions at checkpoint time should appear in the snapshot");
    }

    @Test
    @DisplayName("NQ: checkpoint writes empty list when no transactions are active")
    void nq_checkpoint_writesEmptyListWhenNoActiveTxs() {
        NQCheckpointStrategy strategy = newNQStrategy();
        strategy.checkpoint(1L);

        NQCheckpointRecord record = (NQCheckpointRecord) findMostRecentCheckpoint();
        assertNotNull(record);
        assertTrue(record.getRunningTxNums().isEmpty(),
                "Running tx list should be empty when no transactions are active");
    }

    @Test
    @DisplayName("NQ: checkpoint resets isPendingCheckpoint to false after completing")
    void nq_checkpoint_resetsPendingFlag() {
        NQCheckpointStrategy strategy = newNQStrategy();
        isPendingCheckpoint.set(true);

        strategy.checkpoint(1L);

        assertFalse(isPendingCheckpoint.get(),
                "isPendingCheckpoint should be reset to false after checkpoint completes");
    }

    @Test
    @DisplayName("NQ: checkpoint throws if logMgr is null")
    void nq_checkpoint_throwsIfLogMgrNull() {
        NQCheckpointStrategy strategy = new NQCheckpointStrategy(null, activeTransactions, isPendingCheckpoint);

        assertThrows(NullPointerException.class, () -> strategy.checkpoint(1L),
                "checkpoint should throw NullPointerException if logMgr has not been set");
    }

    @Test
    @DisplayName("NQ: checkpoint snapshot excludes transactions that completed before checkpoint")
    void nq_checkpoint_excludesCompletedTransactions() {
        NQCheckpointStrategy strategy = newNQStrategy();

        strategy.doRegister(1L);
        strategy.doDeregister(1L); // completes before checkpoint

        strategy.doRegister(2L); // still active at checkpoint time

        strategy.checkpoint(99L);

        NQCheckpointRecord record = (NQCheckpointRecord) findMostRecentCheckpoint();
        assertNotNull(record);
        assertFalse(record.getRunningTxNums().contains(1L),
                "Completed transaction should not appear in NQ checkpoint snapshot");
        assertTrue(record.getRunningTxNums().contains(2L),
                "Active transaction should appear in NQ checkpoint snapshot");
    }

    @Test
    @DisplayName("NQ: concurrent register and checkpoint produces consistent snapshot")
    void nq_checkpoint_writeLockPreventsPartialSnapshot() throws InterruptedException {
        // The write lock during checkpoint should freeze the active set so no
        // partial registrations appear in the snapshot
        NQCheckpointStrategy strategy = newNQStrategy();
        int numThreads = 20;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads + 1);

        // Register threads that all try to register concurrently with the checkpoint
        for (long i = 1; i <= numThreads; i++) {
            long txNum = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    strategy.doRegister(txNum);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Checkpoint fires at the same time
        executor.submit(() -> {
            try {
                startLatch.await();
                strategy.checkpoint(99L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        startLatch.countDown();
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS),
                "All registrations should complete without deadlock");
        executor.shutdown();

        // The checkpoint should have completed without throwing and the log should be readable
        LogRecord record = findMostRecentCheckpoint();
        assertNotNull(record, "Checkpoint should have been written despite concurrent registrations");
        assertEquals(LogRecord.NQ_CHECKPOINT, record.getOperator());
    }

    // =========================================================================
    // QCheckpointStrategy
    // =========================================================================

    // -------------------------------------------------------------------------
    // doRegister / doDeregister
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Q: doRegister adds transaction to active set when no checkpoint is pending")
    void q_doRegister_addsToActiveTransactions() {
        QCheckpointStrategy strategy = newQStrategy();
        strategy.doRegister(1L);

        assertTrue(activeTransactions.containsKey(1L),
                "Registered transaction should appear in activeTransactions");
    }

    @Test
    @DisplayName("Q: doDeregister removes transaction from active set")
    void q_doDeregister_removesFromActiveTransactions() {
        QCheckpointStrategy strategy = newQStrategy();
        strategy.doRegister(1L);
        strategy.doDeregister(1L);

        assertFalse(activeTransactions.containsKey(1L),
                "Deregistered transaction should be removed from activeTransactions");
    }

    @Test
    @DisplayName("Q: doRegister blocks new transactions while checkpoint is pending")
    void q_doRegister_blocksWhileCheckpointPending() throws InterruptedException {
        QCheckpointStrategy strategy = newQStrategy();
        isPendingCheckpoint.set(true);

        AtomicBoolean registered = new AtomicBoolean(false);
        CountDownLatch startedLatch = new CountDownLatch(1);

        Thread registerThread = new Thread(() -> {
            startedLatch.countDown();
            strategy.doRegister(1L);
            registered.set(true);
        });

        registerThread.start();
        assertTrue(startedLatch.await(1, TimeUnit.SECONDS), "Thread should start");

        // Give the thread time to block on doRegister
        Thread.sleep(200);
        assertFalse(registered.get(),
                "doRegister should be blocked while isPendingCheckpoint is true");

        // Release the block by completing a checkpoint
        strategy.checkpoint(99L); // this sets isPendingCheckpoint=false and signals

        registerThread.join(2000);
        assertTrue(registered.get(),
                "doRegister should unblock after checkpoint completes");
    }

    // -------------------------------------------------------------------------
    // checkpoint()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Q: checkpoint writes CHECKPOINT record to log")
    void q_checkpoint_writesCheckpointRecord() {
        QCheckpointStrategy strategy = newQStrategy();
        strategy.checkpoint(1L);

        LogRecord record = findMostRecentCheckpoint();
        assertNotNull(record, "A checkpoint record should have been written");
        assertEquals(LogRecord.CHECKPOINT, record.getOperator(),
                "Record should be a Q CHECKPOINT");
    }

    @Test
    @DisplayName("Q: checkpoint waits for active transactions to drain before writing")
    void q_checkpoint_waitsForActiveTxsToDrain() throws InterruptedException {
        QCheckpointStrategy strategy = newQStrategy();

        // Register a transaction that will drain after a short delay
        strategy.doRegister(1L);

        AtomicBoolean checkpointCompleted = new AtomicBoolean(false);
        CountDownLatch checkpointStarted = new CountDownLatch(1);

        Thread checkpointThread = new Thread(() -> {
            checkpointStarted.countDown();
            strategy.checkpoint(99L);
            checkpointCompleted.set(true);
        });

        checkpointThread.start();
        assertTrue(checkpointStarted.await(1, TimeUnit.SECONDS));

        // Give checkpoint thread time to enter the wait loop
        Thread.sleep(200);
        assertFalse(checkpointCompleted.get(),
                "Checkpoint should be waiting for active transaction to drain");

        // Drain the transaction — checkpoint should now complete
        strategy.doDeregister(1L);
        checkpointThread.join(2000);

        assertTrue(checkpointCompleted.get(),
                "Checkpoint should complete after active transaction drains");

        LogRecord record = findMostRecentCheckpoint();
        assertNotNull(record, "Checkpoint record should have been written after drain");
    }

    @Test
    @DisplayName("Q: checkpoint completes immediately when no transactions are active")
    void q_checkpoint_completesImmediatelyWhenNoActiveTxs() {
        QCheckpointStrategy strategy = newQStrategy();

        // Should not block since activeTransactions is empty
        assertTimeoutPreemptively(
                java.time.Duration.ofSeconds(2),
                () -> strategy.checkpoint(1L),
                "Q checkpoint should complete immediately with no active transactions"
        );

        LogRecord record = findMostRecentCheckpoint();
        assertNotNull(record);
        assertEquals(LogRecord.CHECKPOINT, record.getOperator());
    }

    @Test
    @DisplayName("Q: checkpoint resets isPendingCheckpoint to false after completing")
    void q_checkpoint_resetsPendingFlag() {
        QCheckpointStrategy strategy = newQStrategy();
        isPendingCheckpoint.set(true);

        strategy.checkpoint(1L);

        assertFalse(isPendingCheckpoint.get(),
                "isPendingCheckpoint should be reset to false after checkpoint completes");
    }

    @Test
    @DisplayName("Q: checkpoint throws if logMgr is null")
    void q_checkpoint_throwsIfLogMgrNull() {
        QCheckpointStrategy strategy = new QCheckpointStrategy(null, activeTransactions, isPendingCheckpoint);

        assertThrows(NullPointerException.class, () -> strategy.checkpoint(1L),
                "checkpoint should throw NullPointerException if logMgr has not been set");
    }

    @Test
    @DisplayName("Q: multiple transactions must all drain before checkpoint fires")
    void q_checkpoint_waitsForAllActiveTxsToDrain() throws InterruptedException {
        QCheckpointStrategy strategy = newQStrategy();

        strategy.doRegister(1L);
        strategy.doRegister(2L);
        strategy.doRegister(3L);

        AtomicBoolean checkpointCompleted = new AtomicBoolean(false);
        Thread checkpointThread = new Thread(() -> {
            strategy.checkpoint(99L);
            checkpointCompleted.set(true);
        });

        checkpointThread.start();
        Thread.sleep(200);

        assertFalse(checkpointCompleted.get(), "Checkpoint should wait while 3 transactions are active");

        strategy.doDeregister(1L);
        Thread.sleep(100);
        assertFalse(checkpointCompleted.get(), "Checkpoint should still wait with 2 transactions active");

        strategy.doDeregister(2L);
        Thread.sleep(100);
        assertFalse(checkpointCompleted.get(), "Checkpoint should still wait with 1 transaction active");

        strategy.doDeregister(3L);
        checkpointThread.join(2000);
        assertTrue(checkpointCompleted.get(), "Checkpoint should complete after all transactions drain");
    }
}
