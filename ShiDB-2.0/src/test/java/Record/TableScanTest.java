package Record;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import Server.ConfigFetcher;
import Server.ShiDB;
import Transaction.Transaction;
import Transaction.Concurrency.ConcurrencyMgr;
import Transaction.Concurrency.TransactionRegistrySingleton;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TableScannerTest {

    private static final String TEST_DIR    = "TableScanner-unit-test";
    private static final int    BLOCK_SIZE  = 400;
    private static final int    BUFFER_SIZE = 8;

    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger(0);

    private ShiDB db;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @BeforeEach
    void setUp() throws IOException {
        deleteDirectory(TEST_DIR);
        ConcurrencyMgr.reinit();
        TransactionRegistrySingleton.reinit();
        ConfigFetcher.reloadConfig("src/test/resources/defaultTestConfig.json");
        db = new ShiDB(TEST_DIR, BLOCK_SIZE, BUFFER_SIZE);
    }

    @AfterEach
    void tearDown() throws IOException {
        deleteDirectory(TEST_DIR);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private String uniqueTable() {
        return "tbl_" + TABLE_COUNTER.incrementAndGet();
    }

    private Transaction newTx() {
        return new Transaction(db.getFileMgr(), db.getLogMgr(), db.getBufferMgr());
    }

    private Layout buildLayout() {
        Schema sch = new Schema();
        sch.addIntField("A");
        sch.addStringField("B", 9);
        return new Layout(sch);
    }

    private List<RecordID> insertRecords(TableScanner ts, int n) {
        List<RecordID> rids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ts.insert();
            ts.setInt("A", i);
            ts.setString("B", "rec" + i);
            rids.add(ts.getRecordId());
        }
        return rids;
    }

    /** Collect all A-values via hasNext()/next(). */
    private List<Integer> collectAllA(TableScanner ts) {
        List<Integer> values = new ArrayList<>();
        ts.beforeFirst();
        while (ts.hasNext())
            values.add(ts.next().getInt("A"));
        return values;
    }

    /** Collect all RIDs via hasNext()/next(). */
    private List<RecordID> collectAllRids(TableScanner ts) {
        List<RecordID> rids = new ArrayList<>();
        ts.beforeFirst();
        while (ts.hasNext())
            rids.add(ts.next().getRecordId());
        return rids;
    }

    private void deleteDirectory(String dirName) throws IOException {
        Path path = Path.of(dirName);
        if (Files.exists(path))
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
    }

    // =========================================================================
    @Nested
    @DisplayName("Insert and read back")
    class InsertAndRead {

        @Test
        @DisplayName("Inserted int value is readable at same cursor position")
        void insertedIntIsReadable() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            ts.insert();
            ts.setInt("A", 42);
            assertEquals(42, ts.getInt("A"));
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("Inserted string value is readable at same cursor position")
        void insertedStringIsReadable() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            ts.insert();
            ts.setString("B", "hello");
            assertEquals("hello", ts.getString("B"));
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("Multiple fields in same record are independent")
        void multipleFieldsAreIndependent() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            ts.insert();
            ts.setInt("A", 7);
            ts.setString("B", "seven");
            assertEquals(7,       ts.getInt("A"));
            assertEquals("seven", ts.getString("B"));
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("Multiple inserted records all survive a full scan")
        void multipleInsertedRecordsSurviveScan() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            insertRecords(ts, 10);

            List<Integer> found = collectAllA(ts);
            assertEquals(10, found.size());
            for (int i = 0; i < 10; i++)
                assertTrue(found.contains(i), "Missing A=" + i);
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("String at max declared length does not corrupt adjacent field")
        void stringAtMaxLengthDoesNotCorruptAdjacentField() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            ts.insert();
            ts.setInt("A", 99);
            ts.setString("B", "123456789"); // exactly 9 chars
            assertEquals(99,          ts.getInt("A"));
            assertEquals("123456789", ts.getString("B"));
            ts.close();
            tx.commit();
        }
    }

    // =========================================================================
    @Nested
    @DisplayName("Iteration — hasNext() / next()")
    class Iteration {

        @Test
        @DisplayName("hasNext() returns false immediately on an empty table")
        void hasNextReturnsFalseOnEmptyTable() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            ts.beforeFirst();
            assertFalse(ts.hasNext(), "hasNext() should return false on an empty table");
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("next() throws NoSuchElementException on an empty table")
        void nextThrowsOnEmptyTable() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            ts.beforeFirst();
            assertThrows(NoSuchElementException.class, ts::next);
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("hasNext() is idempotent — calling it multiple times without next() does not advance cursor")
        void hasNextIsIdempotent() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            insertRecords(ts, 3);

            ts.beforeFirst();
            assertTrue(ts.hasNext());
            assertTrue(ts.hasNext()); // second call must not advance
            assertTrue(ts.hasNext()); // third call must not advance

            // Only one next() call should be needed to get the first record
            assertEquals(0, ts.next().getInt("A"));
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("beforeFirst() resets cursor — iterating twice yields same record count")
        void beforeFirstResetsCursor() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            insertRecords(ts, 5);

            List<Integer> firstPass  = collectAllA(ts);
            List<Integer> secondPass = collectAllA(ts);

            assertEquals(firstPass.size(), secondPass.size(),
                    "Second iteration after beforeFirst() must visit same number of records");
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("Inserting enough records to span multiple blocks — all are reachable")
        void recordsSpanningMultipleBlocksAreAllReachable() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            insertRecords(ts, 50);

            List<Integer> found = collectAllA(ts);
            assertEquals(50, found.size(), "All records across all blocks must be reachable");
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("hasNext() returns false after all records are deleted")
        void hasNextReturnsFalseAfterAllDeleted() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            insertRecords(ts, 5);

            ts.beforeFirst();
            while (ts.hasNext()) {
                ts.next();
                ts.delete();
            }

            ts.beforeFirst();
            assertFalse(ts.hasNext(), "hasNext() should return false when all records have been deleted");
            ts.close();
            tx.commit();
        }
    }

    // =========================================================================
    @Nested
    @DisplayName("Delete")
    class Delete {

        @Test
        @DisplayName("Deleted slot is skipped by subsequent scan")
        void deletedSlotIsSkipped() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            insertRecords(ts, 5);

            ts.beforeFirst();
            assertTrue(ts.hasNext(), "Table must have at least one record to delete");
            RecordID deletedRid = ts.next().getRecordId();
            ts.delete();

            List<RecordID> remainingRids = collectAllRids(ts);
            assertEquals(4, remainingRids.size(), "Exactly one slot should be skipped after delete");
            assertFalse(remainingRids.contains(deletedRid),
                    "The deleted slot's RID must not appear in scan — in-use flag must be checked");
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("Partial delete leaves surviving records intact")
        void partialDeleteLeavesCorrectRecords() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            insertRecords(ts, 10);

            ts.beforeFirst();
            while (ts.hasNext())
                if (ts.next().getInt("A") % 2 == 0)
                    ts.delete();

            List<Integer> remaining = collectAllA(ts);
            assertEquals(5, remaining.size());
            for (int v : remaining)
                assertEquals(1, v % 2, "Only odd A-values should survive");
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("Deleted slot is reused by a subsequent insert")
        void deletedSlotIsReused() {
            Transaction tx = newTx();
            String table = uniqueTable();
            TableScanner ts = new TableScanner(tx, table, buildLayout());
            insertRecords(ts, 3);
            int blockCountAfterInserts = tx.fileSize(table + ".tbl");

            ts.beforeFirst();
            while (ts.hasNext()) {
                ts.next();
                ts.delete();
            }

            ts.insert();
            int blockCountAfterReinsert = tx.fileSize(table + ".tbl");

            assertEquals(blockCountAfterInserts, blockCountAfterReinsert,
                    "Inserting into a table with free slots must not append a new block");
            ts.close();
            tx.commit();
        }
    }

    // =========================================================================
    @Nested
    @DisplayName("RecordID")
    class RecordIDTests {

        @Test
        @DisplayName("getRecordId() returns a stable identifier that moveToRecordID() can relocate")
        void recordIdIsStableAndRelocatable() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            ts.insert();
            ts.setInt("A", 77);
            ts.setString("B", "lucky");
            RecordID rid = ts.getRecordId();

            insertRecords(ts, 5);
            ts.moveToRecordID(rid);

            assertEquals(77,      ts.getInt("A"),    "Should land on the correct record via RID");
            assertEquals("lucky", ts.getString("B"), "Should land on the correct record via RID");
            ts.close();
            tx.commit();
        }
    }

    // =========================================================================
    @Nested
    @DisplayName("Update in place")
    class UpdateInPlace {

        @Test
        @DisplayName("setInt on an existing record overwrites the previous value")
        void setIntOverwritesPreviousValue() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            ts.insert();
            ts.setInt("A", 1);

            ts.beforeFirst();
            ts.next().setInt("A", 100);

            ts.beforeFirst();
            assertEquals(100, ts.next().getInt("A"), "In-place update must overwrite the old int value");
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("setString on an existing record overwrites the previous value")
        void setStringOverwritesPreviousValue() {
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), buildLayout());
            ts.insert();
            ts.setString("B", "old");

            ts.beforeFirst();
            ts.next().setString("B", "new");

            ts.beforeFirst();
            assertEquals("new", ts.next().getString("B"), "In-place update must overwrite the old string value");
            ts.close();
            tx.commit();
        }
    }

    // =========================================================================
    @Nested
    @DisplayName("Transaction semantics")
    class TransactionSemantics {

        @Test
        @DisplayName("Committed data is visible to a new transaction")
        void committedDataIsVisibleToNewTransaction() {
            String table  = uniqueTable();
            Layout layout = buildLayout();

            Transaction tx1 = newTx();
            TableScanner ts1 = new TableScanner(tx1, table, layout);
            ts1.insert();
            ts1.setInt("A", 55);
            ts1.setString("B", "persist");
            ts1.close();
            tx1.commit();

            Transaction tx2 = newTx();
            TableScanner ts2 = new TableScanner(tx2, table, layout);
            assertTrue(ts2.hasNext(), "New transaction should see committed record");
            TableScanner record = ts2.next();
            assertEquals(55,        record.getInt("A"));
            assertEquals("persist", record.getString("B"));
            ts2.close();
            tx2.commit();
        }

        @Test
        @DisplayName("Rolled-back inserts are not visible after rollback")
        void rolledBackInsertsAreNotVisible() {
            String table  = uniqueTable();
            Layout layout = buildLayout();

            Transaction tx1 = newTx();
            TableScanner ts1 = new TableScanner(tx1, table, layout);
            ts1.insert();
            ts1.setInt("A", 99);
            ts1.close();
            tx1.rollback();

            Transaction tx2 = newTx();
            TableScanner ts2 = new TableScanner(tx2, table, layout);
            ts2.beforeFirst();
            assertFalse(ts2.hasNext(), "No records should be visible after rollback");
            ts2.close();
            tx2.commit();
        }
    }

    // =========================================================================
    @Nested
    @DisplayName("Pin leak detection")
    class PinLeakDetection {

        @Test
        @DisplayName("hasNext()/next() does not leak buffer pins when scanning across multiple blocks")
        void hasNextDoesNotLeakPinsAcrossBlocks() {
            Transaction tx = newTx();
            String table = uniqueTable();
            TableScanner ts = new TableScanner(tx, table, buildLayout());
            insertRecords(ts, 50);
            ts.close();
            tx.commit();

            int availableBeforeScan = db.getBufferMgr().getNumAvailableBuffers();

            Transaction tx2 = newTx();
            TableScanner ts2 = new TableScanner(tx2, table, buildLayout());
            int count = 0;
            while (ts2.hasNext()) {
                ts2.next();
                count++;
            }
            ts2.close();
            tx2.commit();

            assertEquals(50, count, "All 50 records should be reachable");
            assertEquals(availableBeforeScan, db.getBufferMgr().getNumAvailableBuffers(),
                    "Buffer pool must return to its original size after scan — pin leak in findNextRid() if this fails");
        }

        @Test
        @DisplayName("Repeated hasNext() calls without next() do not accumulate pins")
        void repeatedHasNextDoesNotLeakPins() {
            Transaction tx = newTx();
            String table = uniqueTable();
            TableScanner ts = new TableScanner(tx, table, buildLayout());
            insertRecords(ts, 30);
            ts.close();
            tx.commit();

            Transaction tx2 = newTx();
            TableScanner ts2 = new TableScanner(tx2, table, buildLayout());

            int availableAfterOpen = db.getBufferMgr().getNumAvailableBuffers();

            ts2.hasNext();
            ts2.hasNext();
            ts2.hasNext();

            assertEquals(availableAfterOpen, db.getBufferMgr().getNumAvailableBuffers(),
                    "Repeated hasNext() calls must not accumulate pins");
            ts2.close();
            tx2.commit();
        }
    }

    // =========================================================================
    @Nested
    @DisplayName("All supported field types")
    class AllFieldTypes {

        @Test
        @DisplayName("Boolean field round-trips correctly")
        void booleanFieldRoundTrips() {
            Schema sch = new Schema();
            sch.addBooleanField("flag");
            Layout l = new Layout(sch);

            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), l);
            ts.insert();
            ts.setBoolean("flag", true);
            assertTrue(ts.getBoolean("flag"));
            ts.insert();
            ts.setBoolean("flag", false);
            assertFalse(ts.getBoolean("flag"));
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("Long field round-trips correctly")
        void longFieldRoundTrips() {
            Schema sch = new Schema();
            sch.addLongField("bignum");
            Layout l = new Layout(sch);

            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), l);
            ts.insert();
            ts.setLong("bignum", Long.MAX_VALUE);
            assertEquals(Long.MAX_VALUE, ts.getLong("bignum"));
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("Double field round-trips correctly")
        void doubleFieldRoundTrips() {
            Schema sch = new Schema();
            sch.addDoubleField("score");
            Layout l = new Layout(sch);

            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), l);
            ts.insert();
            ts.setDouble("score", 3.14159);
            assertEquals(3.14159, ts.getDouble("score"), 1e-9);
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("DateTime field round-trips correctly")
        void dateTimeFieldRoundTrips() {
            Schema sch = new Schema();
            sch.addDateTimeField("created");
            Layout l = new Layout(sch);

            LocalDateTime now = LocalDateTime.of(2024, 6, 15, 10, 30, 0);
            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), l);
            ts.insert();
            ts.setDateTime("created", now);
            assertEquals(now, ts.getDateTime("created"));
            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("Byte field round-trips correctly — zero, max signed, and 0xFF")
        void byteFieldRoundTrips() {
            Schema sch = new Schema();
            sch.addByteField("flags");
            Layout l = new Layout(sch);

            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), l);

            ts.insert();
            ts.setByte("flags", (byte) 0x00);
            assertEquals((byte) 0x00, ts.getByte("flags"), "Zero byte should round-trip");

            ts.insert();
            ts.setByte("flags", (byte) 0x7F);
            assertEquals((byte) 0x7F, ts.getByte("flags"), "Max signed byte should round-trip");

            ts.insert();
            ts.setByte("flags", (byte) -1);  // 0xFF
            assertEquals((byte) -1, ts.getByte("flags"), "0xFF (-1 signed) should round-trip without sign corruption");

            ts.close();
            tx.commit();
        }

        @Test
        @DisplayName("Byte field is independent of adjacent fields")
        void byteFieldDoesNotCorruptAdjacentFields() {
            Schema sch = new Schema();
            sch.addIntField("before");
            sch.addByteField("flags");
            sch.addIntField("after");
            Layout l = new Layout(sch);

            Transaction tx = newTx();
            TableScanner ts = new TableScanner(tx, uniqueTable(), l);
            ts.insert();
            ts.setInt("before", 111);
            ts.setByte("flags", (byte) 0x55);
            ts.setInt("after", 222);

            assertEquals(111,         ts.getInt("before"),  "Field before byte must be unaffected");
            assertEquals((byte) 0x55, ts.getByte("flags"),  "Byte field must hold written value");
            assertEquals(222,         ts.getInt("after"),   "Field after byte must be unaffected");
            ts.close();
            tx.commit();
        }
    }
}