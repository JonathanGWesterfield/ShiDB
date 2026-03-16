package Transaction;

import Log.LogMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import Server.ConfigFetcher;
import Server.ShiDB;
import Transaction.Concurrency.TransactionRegistrySingleton;
import Transaction.Recovery.LogRecord;
import Transaction.Recovery.LogRecordFactory;
import Transaction.Recovery.RecordType.NQCheckpointRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionRegistrySingletonTest {

    private static final String TEST_DIR = "TransactionRegistry-unit-test";

    private ShiDB shiDB;
    private LogMgr logMgr;
    private TransactionRegistrySingleton registry;

    @BeforeEach
    void setUp() throws IOException {
        ConfigFetcher.reloadConfig("src/test/resources/concurrencyTestConfig.json");
        TransactionRegistrySingleton.reinit();

        shiDB = new ShiDB(TEST_DIR, 400);
        logMgr = shiDB.getLogMgr();

        registry = TransactionRegistrySingleton.getInstance();
        registry.setLogMgr(logMgr);

        clearLogFile();
    }

    @AfterEach
    void cleanUp() throws IOException {
        TransactionRegistrySingleton.reinit();
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

    /** Reads all log records from newest to oldest and returns the first checkpoint record found, or null. */
    private LogRecord findMostRecentCheckpoint() {
        Iterator<byte[]> iter = logMgr.iterator();
        while (iter.hasNext()) {
            LogRecord record = LogRecordFactory.convertToLogRecord(iter.next());
            if (record.getOperator() == LogRecord.CHECKPOINT || record.getOperator() == LogRecord.NQ_CHECKPOINT)
                return record;
        }
        return null;
    }

    /** Counts how many checkpoint records appear in the log. */
    private int countCheckpoints() {
        int count = 0;
        Iterator<byte[]> iter = logMgr.iterator();
        while (iter.hasNext()) {
            LogRecord record = LogRecordFactory.convertToLogRecord(iter.next());
            if (record.getOperator() == LogRecord.CHECKPOINT || record.getOperator() == LogRecord.NQ_CHECKPOINT)
                count++;
        }
        return count;
    }

    // -------------------------------------------------------------------------
    // Singleton behavior
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("getInstance always returns the same instance")
    void getInstance_returnsSameInstance() {
        TransactionRegistrySingleton a = TransactionRegistrySingleton.getInstance();
        TransactionRegistrySingleton b = TransactionRegistrySingleton.getInstance();
        assertSame(a, b, "getInstance should always return the same singleton instance");
    }

    @Test
    @DisplayName("reinit produces a fresh instance")
    void reinit_producesFreshInstance() {
        TransactionRegistrySingleton before = TransactionRegistrySingleton.getInstance();
        TransactionRegistrySingleton.reinit();
        TransactionRegistrySingleton after = TransactionRegistrySingleton.getInstance();
        assertNotSame(before, after, "reinit should produce a new instance");
    }

    // -------------------------------------------------------------------------
    // Checkpoint threshold
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Checkpoint fires exactly at N commits — not before")
    void checkpoint_firesAtThreshold() {
        int n = ConfigFetcher.getNumTransactionsPerCheckpoint();

        // Complete N-1 transactions — no checkpoint should fire yet
        for (long txNum = 1; txNum < n; txNum++) {
            registry.registerTx(txNum);
            registry.deRegisterTx(txNum);
        }
        assertEquals(0, countCheckpoints(),
                "No checkpoint should fire before reaching the threshold");

        // Complete the Nth transaction — checkpoint should fire now
        registry.registerTx(n);
        registry.deRegisterTx(n);

        assertEquals(1, countCheckpoints(),
                "Exactly one checkpoint should fire at the Nth commit");
    }

    @Test
    @DisplayName("Checkpoint counter resets after firing — fires again after another N commits")
    void checkpoint_counterResetsAfterFiring() {
        int n = ConfigFetcher.getNumTransactionsPerCheckpoint();

        // First batch — trigger first checkpoint
        for (long txNum = 1; txNum <= n; txNum++) {
            registry.registerTx(txNum);
            registry.deRegisterTx(txNum);
        }
        assertEquals(1, countCheckpoints(), "First checkpoint should have fired");

        // Second batch — trigger second checkpoint
        for (long txNum = n + 1; txNum <= n * 2; txNum++) {
            registry.registerTx(txNum);
            registry.deRegisterTx(txNum);
        }
        assertEquals(2, countCheckpoints(),
                "Second checkpoint should fire after another N commits");
    }

    @Test
    @DisplayName("No checkpoint fires if transactions never complete")
    void checkpoint_doesNotFireWithoutDeregister() {
        int n = ConfigFetcher.getNumTransactionsPerCheckpoint();

        // Register N transactions but never deregister them
        for (long txNum = 1; txNum <= n; txNum++)
            registry.registerTx(txNum);

        assertEquals(0, countCheckpoints(),
                "Checkpoint should not fire without deregistrations");
    }

    // -------------------------------------------------------------------------
    // NQ checkpoint snapshot correctness
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("NQ checkpoint records in-flight transactions at snapshot time")
    void nqCheckpoint_recordsInFlightTransactions() {
        int n = ConfigFetcher.getNumTransactionsPerCheckpoint();

        // Complete N-1 transactions to prime the counter
        for (long txNum = 1; txNum < n; txNum++) {
            registry.registerTx(txNum);
            registry.deRegisterTx(txNum);
        }

        // Start an in-flight transaction that will be active when the checkpoint fires
        long inFlightTxNum = 100L;
        registry.registerTx(inFlightTxNum);

        // Complete the Nth transaction to trigger the checkpoint
        // inFlightTxNum is still active at this point
        registry.registerTx(n);
        registry.deRegisterTx(n);

        LogRecord checkpoint = findMostRecentCheckpoint();
        assertNotNull(checkpoint, "A checkpoint should have been written");
        assertEquals(LogRecord.NQ_CHECKPOINT, checkpoint.getOperator(),
                "Should be an NQ checkpoint");

        NQCheckpointRecord nqRecord = (NQCheckpointRecord) checkpoint;
        assertTrue(nqRecord.getRunningTxNums().contains(inFlightTxNum),
                "In-flight transaction should appear in the NQ checkpoint's running tx list");

        // Cleanup
        registry.deRegisterTx(inFlightTxNum);
    }

    @Test
    @DisplayName("NQ checkpoint records empty list when no transactions are in-flight")
    void nqCheckpoint_emptyListWhenNoInFlightTransactions() {
        int n = ConfigFetcher.getNumTransactionsPerCheckpoint();

        // Complete exactly N transactions with no overlap
        for (long txNum = 1; txNum <= n; txNum++) {
            registry.registerTx(txNum);
            registry.deRegisterTx(txNum);
        }

        LogRecord checkpoint = findMostRecentCheckpoint();
        assertNotNull(checkpoint);
        assertEquals(LogRecord.NQ_CHECKPOINT, checkpoint.getOperator());

        NQCheckpointRecord nqRecord = (NQCheckpointRecord) checkpoint;
        assertTrue(nqRecord.getRunningTxNums().isEmpty(),
                "No in-flight transactions means the running tx list should be empty");
    }

    @Test
    @DisplayName("NQ checkpoint does not include transactions that completed before snapshot")
    void nqCheckpoint_doesNotIncludeCompletedTransactions() {
        int n = ConfigFetcher.getNumTransactionsPerCheckpoint();

        // Complete N-1 transactions
        for (long txNum = 1; txNum < n; txNum++) {
            registry.registerTx(txNum);
            registry.deRegisterTx(txNum);
        }

        // Start and complete a transaction before the checkpoint fires
        long completedBeforeCheckpoint = 99L;
        registry.registerTx(completedBeforeCheckpoint);
        registry.deRegisterTx(completedBeforeCheckpoint);

        // Trigger checkpoint
        registry.registerTx(n);
        registry.deRegisterTx(n);

        NQCheckpointRecord nqRecord = (NQCheckpointRecord) findMostRecentCheckpoint();
        assertFalse(nqRecord.getRunningTxNums().contains(completedBeforeCheckpoint),
                "Transaction completed before checkpoint should not appear in running tx list");
    }

    // -------------------------------------------------------------------------
    // Concurrency: double-checkpoint prevention
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("compareAndSet prevents multiple threads from triggering simultaneous checkpoints")
    void checkpoint_onlyFiresOnceWhenMultipleThreadsHitThreshold() throws InterruptedException {
        // Reload with n=10 so exactly numThreads deregistrations hit the threshold once
        ConfigFetcher.getConfigs("src/test/resources/concurrencyTestConfig.json");
        TransactionRegistrySingleton.reinit();
        registry = TransactionRegistrySingleton.getInstance();
        registry.setLogMgr(logMgr);

        int numThreads = 10;
        long baseTxNum = 1000L;
        for (int i = 0; i < numThreads; i++)
            registry.registerTx(baseTxNum + i);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < numThreads; i++) {
            long txNum = baseTxNum + i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    registry.deRegisterTx(txNum);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(1, countCheckpoints(),
                "Exactly one checkpoint should fire even when multiple threads hit the threshold simultaneously");
    }
}