package recovery;

import file.BlockId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import server.ConfigFetcher;
import transaction.recovery.LogRecord;
import transaction.recovery.LogRecordFactory;
import transaction.recovery.recordtype.*;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that every log record type correctly serializes via toBytes() and deserializes
 * back via LogRecordFactory. No LogMgr or ShiDB needed — pure serialization round-trips.
 *
 * Catches bugs in:
 *  - toBytes() / writeToLog() byte layout
 *  - DataLogRecordHeader position calculations
 *  - NQCheckpointRecord byte array allocation
 *  - Page.getBytes() offset handling
 */
public class LogRecordSerializationTest {

    private static final BlockId TEST_BLOCK = new BlockId("testfile", 3);
    private static final int OFFSET = 8;
    private static final long TX_NUM = 42L;

    @BeforeEach
    void setUp() {
        ConfigFetcher.reloadConfig("src/test/resources/undoOnlyRecoveryTestConfig.json");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private LogRecord roundTrip(LogRecord record) {
        return LogRecordFactory.convertToLogRecord(record.toBytes());
    }

    private void useUndoRedoConfig() {
        ConfigFetcher.reloadConfig("src/test/resources/undoRedoRecoveryTestConfig.json");
    }

    // -------------------------------------------------------------------------
    // Simple records
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("StartRecord round-trips correctly")
    void startRecord_roundTrip() {
        LogRecord parsed = roundTrip(new StartRecord(TX_NUM));

        assertEquals(LogRecord.START, parsed.getOperator());
        assertEquals(TX_NUM, parsed.getTxNum());
    }

    @Test
    @DisplayName("CommitRecord round-trips correctly")
    void commitRecord_roundTrip() {
        LogRecord parsed = roundTrip(new CommitRecord(TX_NUM));

        assertEquals(LogRecord.COMMIT, parsed.getOperator());
        assertEquals(TX_NUM, parsed.getTxNum());
    }

    @Test
    @DisplayName("RollbackRecord round-trips correctly")
    void rollbackRecord_roundTrip() {
        LogRecord parsed = roundTrip(new RollbackRecord(TX_NUM));

        assertEquals(LogRecord.ROLLBACK, parsed.getOperator());
        assertEquals(TX_NUM, parsed.getTxNum());
    }

    // -------------------------------------------------------------------------
    // Checkpoint records
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("CheckpointRecord round-trips correctly")
    void checkpointRecord_roundTrip() {
        LogRecord parsed = roundTrip(new CheckpointRecord(TX_NUM));

        assertEquals(LogRecord.CHECKPOINT, parsed.getOperator());
    }

    @Test
    @DisplayName("NQCheckpointRecord round-trips with empty running tx list")
    void nqCheckpointRecord_emptyList_roundTrip() {
        LogRecord parsed = roundTrip(new NQCheckpointRecord(TX_NUM, new ArrayList<>()));

        assertEquals(LogRecord.NQ_CHECKPOINT, parsed.getOperator());
        assertTrue(((NQCheckpointRecord) parsed).getRunningTxNums().isEmpty());
    }

    @Test
    @DisplayName("NQCheckpointRecord round-trips with populated running tx list")
    void nqCheckpointRecord_withTxNums_roundTrip() {
        ArrayList<Long> runningTxs = new ArrayList<>(List.of(1L, 5L, 99L));
        LogRecord parsed = roundTrip(new NQCheckpointRecord(TX_NUM, runningTxs));

        assertEquals(LogRecord.NQ_CHECKPOINT, parsed.getOperator());
        assertEquals(runningTxs, ((NQCheckpointRecord) parsed).getRunningTxNums());
    }

    @Test
    @DisplayName("NQCheckpointRecord does not throw with large tx list — catches byte allocation bug")
    void nqCheckpointRecord_largeTxList_doesNotThrow() {
        // Catches the bug where toBytes() allocated only 12 bytes (operator + txNum)
        // then tried to write list data past the end of the array
        ArrayList<Long> runningTxs = new ArrayList<>(List.of(10L, 20L, 30L, 40L, 50L));
        assertDoesNotThrow(() -> roundTrip(new NQCheckpointRecord(TX_NUM, runningTxs)));
    }

    // -------------------------------------------------------------------------
    // SetInt
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("SetIntRecord (UNDO_ONLY) round-trips correctly")
    void setIntRecord_undoOnly_roundTrip() {
        SetIntRecord parsed = (SetIntRecord) roundTrip(
                new SetIntRecord(TX_NUM, TEST_BLOCK, OFFSET, 123, null));

        assertEquals(LogRecord.SET_INT, parsed.getOperator());
        assertEquals(TX_NUM, parsed.getTxNum());
        assertEquals(TEST_BLOCK, parsed.getInner().getBlock());
        assertEquals(OFFSET, parsed.getInner().getOffset());
        assertEquals(123, parsed.getInner().getOldValue());
    }

    @Test
    @DisplayName("SetIntRecord (UNDO_REDO) round-trips correctly")
    void setIntRecord_undoRedo_roundTrip() {
        useUndoRedoConfig();
        SetIntRecord parsed = (SetIntRecord) roundTrip(
                new SetIntRecord(TX_NUM, TEST_BLOCK, OFFSET, 123, 456));

        assertEquals(TEST_BLOCK, parsed.getInner().getBlock());
        assertEquals(OFFSET, parsed.getInner().getOffset());
        assertEquals(123, parsed.getInner().getOldValue());
        assertEquals(456, parsed.getInner().getNewValue());
    }

    // -------------------------------------------------------------------------
    // SetString
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("SetStringRecord (UNDO_ONLY) round-trips correctly")
    void setStringRecord_undoOnly_roundTrip() {
        SetStringRecord parsed = (SetStringRecord) roundTrip(
                new SetStringRecord(TX_NUM, TEST_BLOCK, OFFSET, "hello", null));

        assertEquals(LogRecord.SET_STRING, parsed.getOperator());
        assertEquals(TX_NUM, parsed.getTxNum());
        assertEquals(TEST_BLOCK, parsed.getInner().getBlock());
        assertEquals(OFFSET, parsed.getInner().getOffset());
        assertEquals("hello", parsed.getInner().getOldValue());
    }

    @Test
    @DisplayName("SetStringRecord (UNDO_REDO) round-trips correctly")
    void setStringRecord_undoRedo_roundTrip() {
        useUndoRedoConfig();
        SetStringRecord parsed = (SetStringRecord) roundTrip(
                new SetStringRecord(TX_NUM, TEST_BLOCK, OFFSET, "hello", "world"));

        assertEquals("hello", parsed.getInner().getOldValue());
        assertEquals("world", parsed.getInner().getNewValue());
    }

    @Test
    @DisplayName("SetStringRecord handles empty string correctly")
    void setStringRecord_emptyString_roundTrip() {
        SetStringRecord parsed = (SetStringRecord) roundTrip(
                new SetStringRecord(TX_NUM, TEST_BLOCK, OFFSET, "", null));

        assertEquals("", parsed.getInner().getOldValue());
    }

    // -------------------------------------------------------------------------
    // SetShort
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("SetShortRecord (UNDO_ONLY) round-trips correctly")
    void setShortRecord_undoOnly_roundTrip() {
        SetShortRecord parsed = (SetShortRecord) roundTrip(
                new SetShortRecord(TX_NUM, TEST_BLOCK, OFFSET, (short) 777, null));

        assertEquals(LogRecord.SET_SHORT, parsed.getOperator());
        assertEquals(TEST_BLOCK, parsed.getInner().getBlock());
        assertEquals(OFFSET, parsed.getInner().getOffset());
        assertEquals((short) 777, parsed.getInner().getOldValue());
    }

    // -------------------------------------------------------------------------
    // SetByte
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("SetByteRecord (UNDO_ONLY) round-trips correctly")
    void setByteRecord_undoOnly_roundTrip() {
        SetByteRecord parsed = (SetByteRecord) roundTrip(
                new SetByteRecord(TX_NUM, TEST_BLOCK, OFFSET, (byte) 0x0F, null));

        assertEquals(LogRecord.SET_BYTE, parsed.getOperator());
        assertEquals((byte) 0x0F, parsed.getInner().getOldValue());
    }

    // -------------------------------------------------------------------------
    // SetBoolean
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("SetBooleanRecord (UNDO_ONLY) round-trips correctly — true")
    void setBooleanRecord_true_roundTrip() {
        SetBooleanRecord parsed = (SetBooleanRecord) roundTrip(
                new SetBooleanRecord(TX_NUM, TEST_BLOCK, OFFSET, true, null));

        assertEquals(LogRecord.SET_BOOLEAN, parsed.getOperator());
        assertTrue(parsed.getInner().getOldValue());
    }

    @Test
    @DisplayName("SetBooleanRecord (UNDO_ONLY) round-trips correctly — false")
    void setBooleanRecord_false_roundTrip() {
        SetBooleanRecord parsed = (SetBooleanRecord) roundTrip(
                new SetBooleanRecord(TX_NUM, TEST_BLOCK, OFFSET, false, null));

        assertFalse(parsed.getInner().getOldValue());
    }

    // -------------------------------------------------------------------------
    // SetLong
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("SetLongRecord (UNDO_ONLY) round-trips correctly")
    void setLongRecord_undoOnly_roundTrip() {
        SetLongRecord parsed = (SetLongRecord) roundTrip(
                new SetLongRecord(TX_NUM, TEST_BLOCK, OFFSET, Long.MAX_VALUE, null));

        assertEquals(LogRecord.SET_LONG, parsed.getOperator());
        assertEquals(Long.MAX_VALUE, parsed.getInner().getOldValue());
    }

    // -------------------------------------------------------------------------
    // SetDouble
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("SetDoubleRecord (UNDO_ONLY) round-trips correctly")
    void setDoubleRecord_undoOnly_roundTrip() {
        double oldValue = 3.141592653589793;
        SetDoubleRecord parsed = (SetDoubleRecord) roundTrip(
                new SetDoubleRecord(TX_NUM, TEST_BLOCK, OFFSET, oldValue, null));

        assertEquals(LogRecord.SET_DOUBLE, parsed.getOperator());
        assertEquals(oldValue, parsed.getInner().getOldValue(), 0.000001);
    }

    // -------------------------------------------------------------------------
    // SetDateTime
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("SetDateTimeRecord (UNDO_ONLY) round-trips correctly")
    void setDateTimeRecord_undoOnly_roundTrip() {
        // Truncate to seconds — epoch storage loses sub-second precision
        LocalDateTime oldValue = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
        SetDateTimeRecord parsed = (SetDateTimeRecord) roundTrip(
                new SetDateTimeRecord(TX_NUM, TEST_BLOCK, OFFSET, oldValue, null));

        assertEquals(LogRecord.SET_DATETIME, parsed.getOperator());
        assertEquals(oldValue, parsed.getInner().getOldValue());
    }

    // -------------------------------------------------------------------------
    // Cross-cutting: DataLogRecordHeader parsing
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("DataLogRecordHeader parses block, txNum, and offset correctly")
    void dataRecordHeader_allFields_parsedCorrectly() {
        BlockId block = new BlockId("anotherfile", 7);
        long txNum = 999L;
        int offset = 16;

        SetIntRecord parsed = (SetIntRecord) roundTrip(
                new SetIntRecord(txNum, block, offset, 42, null));

        assertEquals(txNum, parsed.getTxNum());
        assertEquals(block, parsed.getInner().getBlock());
        assertEquals(offset, parsed.getInner().getOffset());
        assertEquals(42, parsed.getInner().getOldValue());
    }

    @Test
    @DisplayName("DataLogRecordHeader parses long filename correctly — catches position offset bugs")
    void dataRecordHeader_longFilename_parsedCorrectly() {
        // Longer filenames shift all subsequent field positions in the byte layout.
        // This directly catches off-by-one errors in DataLogRecordHeader position calculations.
        BlockId block = new BlockId("averylongdatabasefilename", 0);
        SetIntRecord parsed = (SetIntRecord) roundTrip(
                new SetIntRecord(TX_NUM, block, OFFSET, 42, null));

        assertEquals(block, parsed.getInner().getBlock());
        assertEquals(OFFSET, parsed.getInner().getOffset());
        assertEquals(42, parsed.getInner().getOldValue());
    }

    @Test
    @DisplayName("DataLogRecordHeader parses single char filename correctly")
    void dataRecordHeader_shortFilename_parsedCorrectly() {
        BlockId block = new BlockId("x", 0);
        SetIntRecord parsed = (SetIntRecord) roundTrip(
                new SetIntRecord(TX_NUM, block, OFFSET, 99, null));

        assertEquals(block, parsed.getInner().getBlock());
        assertEquals(99, parsed.getInner().getOldValue());
    }
}