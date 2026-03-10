package transaction.recovery.recordtype;

import file.BlockId;
import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.ConfigFetcher;
import transaction.ShouldLog;
import transaction.Transaction;
import transaction.recovery.DataLogRecordHeader;
import transaction.recovery.LogRecord;

import java.time.LocalDateTime;

/* TODO: If in the future, we want to implement multiversion locking, then each of these log records also needs to
    store a timestamp of when the record was written. Not hard, but need to refactor the writeToLog methods to take a
    timestamp as well as the constructor
 */

@Getter
@Slf4j(topic = "RecoveryMgr")
public class SetValueRecord<T> implements LogRecord {
    private final int operator;

    private final PageCodec<T> codec;

    private final boolean isDataRecord = true;

    private long txNum;

    private BlockId block;

    private T oldValue, newValue;

    private int offset;

    // Test/serialization constructor
    @TestOnly
    public SetValueRecord(int operator, PageCodec<T> codec, long txNum, BlockId block, int offset, T oldValue, T newValue) {
        this.operator = operator;
        this.codec = codec;
        this.txNum = txNum;
        this.block = block;
        this.offset = offset;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    public SetValueRecord(int operator, PageCodec<T> codec, Page page) {
        this.operator = operator;
        this.codec = codec;

        DataLogRecordHeader header = new DataLogRecordHeader(page);
        this.txNum = header.getTxNum();
        this.block = header.getBlock();
        int base = header.getValueAreaStart();

        log.debug("Loading up record according to the {} strategy", ConfigFetcher.getRecoveryMgrStrategy().toString());
        switch (ConfigFetcher.getRecoveryMgrStrategy()) {
            case REDO_ONLY -> {
                offset = page.getInt(base);
                newValue = codec.read(page, base + Integer.BYTES);
            }
            case UNDO_REDO -> {
                offset = page.getInt(base);
                oldValue = codec.read(page, base + Integer.BYTES);
                int newOffsetPosition = base + Integer.BYTES + codec.byteSize(oldValue);
                newValue = codec.read(page, newOffsetPosition);
            }
            default -> { // Default is the UNDO_ONLY strategy since that's what the textbook implements
                offset = page.getInt(base);
                oldValue = codec.read(page, base + Integer.BYTES);
            }
        }

        log.debug("Loaded up log record: {}", this.toString());
    }

    @Override
    public String toString() {
        return DataLogRecordHeader.recordToString(operator, txNum, block, offset, offset, oldValue,
                newValue);
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(block);
        switch (operator) {
            case LogRecord.SET_INT -> tx.setInt(block, offset, (Integer) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_STRING -> tx.setString(block, offset, (String) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_BYTE -> tx.setByte(block, offset, (Byte) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_BOOLEAN -> tx.setBoolean(block, offset, (Boolean) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_SHORT -> tx.setShort(block, offset, (Short) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_LONG -> tx.setLong(block, offset, (Long) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_DOUBLE -> tx.setDouble(block, offset, (Double) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_DATETIME -> tx.setDateTime(block, offset, (LocalDateTime) oldValue, ShouldLog.DO_NOT_LOG);
            default -> throw new RuntimeException("Unsupported operator: " + operator);
        }
        tx.unPin(block);
    }

    @Override
    public void redo(Transaction tx) {
        tx.pin(block);
        switch (operator) {
            case LogRecord.SET_INT -> tx.setInt(block, offset, (Integer) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_STRING -> tx.setString(block, offset, (String) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_BYTE -> tx.setByte(block, offset, (Byte) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_BOOLEAN -> tx.setBoolean(block, offset, (Boolean) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_SHORT -> tx.setShort(block, offset, (Short) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_LONG -> tx.setLong(block, offset, (Long) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_DOUBLE -> tx.setDouble(block, offset, (Double) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_DATETIME -> tx.setDateTime(block, offset, (LocalDateTime) newValue, ShouldLog.DO_NOT_LOG);
            default -> throw new RuntimeException("Unsupported operator: " + operator);
        }
        tx.unPin(block);
    }

    public byte[] toBytes() {
        return switch (ConfigFetcher.getRecoveryMgrStrategy()) {
            case REDO_ONLY -> LogRecord.toBytes(operator, txNum, block, offset, codec.byteSize(newValue),
                    new ValueWriter((p, pos) -> codec.write(p, pos, newValue), String.valueOf(newValue)));
            case UNDO_REDO -> LogRecord.toBytes(operator, txNum, block, offset, codec.byteSize(oldValue), codec.byteSize(newValue),
                    new ValueWriter((p, pos) -> codec.write(p, pos, oldValue), String.valueOf(oldValue)),
                    new ValueWriter((p, pos) -> codec.write(p, pos, newValue), String.valueOf(newValue)));
            default -> LogRecord.toBytes(operator, txNum, block, offset, codec.byteSize(oldValue),
                    new ValueWriter((p, pos) -> codec.write(p, pos, oldValue), String.valueOf(oldValue)));
        };
    }

    /* Writing to log (undo-only / redo-only). The data format for undo and redo only is the same, it just depends
     on if the RecoveryMgr interprets the value as the old value or the new value, to undo or redo respectively

     UNDO_ONLY and REDO_ONLY records are laid out as such:
        <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (T)>
     */
    public static <T> long writeToLog(LogMgr logMgr, int operator, PageCodec<T> codec, long txNum, BlockId block,
                                      int offset, T value) {

        ValueWriter valueWriter = new ValueWriter((p, pos) -> codec.write(p, pos, value), String.valueOf(value));

        Logger log = LoggerFactory.getLogger("RecoverMgr");
        log.debug("Writing log record: <{}, tx: {}, block: {}, offset: {}, value: {}>",
                LogRecord.operatorToString(operator), txNum, block, offset, valueWriter.strValue());

        return logMgr.appendRecord(LogRecord.toBytes(operator, txNum, block, offset, codec.byteSize(value),
                valueWriter));
    }

    /* Writing to log (undo-redo)

     The UNDO-REDO logs will look like this
     <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), oldValue (T), newValue (T)>
     */
    public static <T> long writeToLog(LogMgr logMgr, int operator, PageCodec<T> codec, long txNum, BlockId block,
                                      int offset, T oldValue, T newValue) {
        ValueWriter oldValueWriter = new ValueWriter((p, pos) -> codec.write(p, pos, oldValue), String.valueOf(oldValue));
        ValueWriter newValueWriter = new ValueWriter((p, pos) -> codec.write(p, pos, newValue), String.valueOf(newValue));

        Logger log = LoggerFactory.getLogger("RecoverMgr");
        log.debug("Writing log record: <{}, tx: {}, block: {}, offset: {}, oldValue: {}, newValue: {} >",
                LogRecord.operatorToString(operator), txNum, block, offset, oldValueWriter.strValue(),
                newValueWriter.strValue());

        return logMgr.appendRecord(LogRecord.toBytes(operator, txNum, block, offset,
                codec.byteSize(oldValue), codec.byteSize(newValue), oldValueWriter, newValueWriter));
    }
}
