package transaction.recovery.recordtype;

import file.BlockId;
import file.Page;
import log.LogMgr;
import lombok.Getter;
import server.ConfigFetcher;
import transaction.ShouldLog;
import transaction.Transaction;
import transaction.recovery.DataLogRecordHeader;
import transaction.recovery.LogRecord;

import java.time.LocalDateTime;

@Getter
public class SetValueRecord<T> implements LogRecord {
    private final int operator;

    private final PageCodec<T> codec;

    private final boolean isDataRecord = true;

    private long txNum;

    private BlockId block;

    private T oldValue, newValue;

    private int oldValueOffset, newValueOffset;

    public SetValueRecord(int operator, PageCodec<T> codec, Page page) {
        this.operator = operator;
        this.codec = codec;

        DataLogRecordHeader header = new DataLogRecordHeader(page);
        this.txNum = header.getTxNum();
        this.block = header.getBlock();
        int base = header.getValueAreaStart();

        switch (ConfigFetcher.getRecoveryMgrStrategy()) {
            case REDO_ONLY -> {
                newValueOffset = page.getInt(base);
                newValue = codec.read(page, base + Integer.BYTES);
            }
            case UNDO_REDO -> {
                oldValueOffset = page.getInt(base);
                oldValue = codec.read(page, base + Integer.BYTES);
                int newOffsetPosition = base + Integer.BYTES + codec.byteSize(oldValue);
                newValueOffset = page.getInt(newOffsetPosition);
                newValue = codec.read(page, newOffsetPosition + Integer.BYTES);
            }
            default -> { // Default is the UNDO_ONLY strategy since that's what the textbook implements
                oldValueOffset = page.getInt(base);
                oldValue = codec.read(page, base + Integer.BYTES);

            }
        }
    }

    @Override
    public String toString() {
        return DataLogRecordHeader.recordToString(operator, txNum, block, oldValueOffset, newValueOffset, oldValue,
                newValue);
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(block);
        switch (operator) {
            case LogRecord.SET_INT -> tx.setInt(block, oldValueOffset, (Integer) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_STRING -> tx.setString(block, oldValueOffset, (String) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_BYTE -> tx.setByte(block, oldValueOffset, (Byte) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_BOOLEAN -> tx.setBoolean(block, oldValueOffset, (Boolean) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_SHORT -> tx.setShort(block, oldValueOffset, (Short) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_LONG -> tx.setLong(block, oldValueOffset, (Long) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_DOUBLE -> tx.setDouble(block, oldValueOffset, (Double) oldValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_DATETIME -> tx.setDateTime(block, oldValueOffset, (LocalDateTime) oldValue, ShouldLog.DO_NOT_LOG);
            default -> throw new RuntimeException("Unsupported operator: " + operator);
        }
        tx.unPin(block);
    }

    @Override
    public void redo(Transaction tx) {
        tx.pin(block);
        switch (operator) {
            case LogRecord.SET_INT -> tx.setInt(block, newValueOffset, (Integer) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_STRING -> tx.setString(block, newValueOffset, (String) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_BYTE -> tx.setByte(block, newValueOffset, (Byte) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_BOOLEAN -> tx.setBoolean(block, newValueOffset, (Boolean) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_SHORT -> tx.setShort(block, newValueOffset, (Short) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_LONG -> tx.setLong(block, newValueOffset, (Long) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_DOUBLE -> tx.setDouble(block, newValueOffset, (Double) newValue, ShouldLog.DO_NOT_LOG);
            case LogRecord.SET_DATETIME -> tx.setDateTime(block, newValueOffset, (LocalDateTime) newValue, ShouldLog.DO_NOT_LOG);
            default -> throw new RuntimeException("Unsupported operator: " + operator);
        }
        tx.unPin(block);
    }

    /* Writing to log (undo-only / redo-only). The data format for undo and redo only is the same, it just depends
     on if the RecoveryMgr interprets the value as the old value or the new value, to undo or redo respectively

     UNDO_ONLY and REDO_ONLY records are laid out as such:
        <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (T)>
     */
    public static <T> long writeToLog(LogMgr logMgr, int operator, PageCodec<T> codec, long txNum, BlockId block,
                                      int offset, T value) {
        return LogRecord.writeToLog(logMgr, operator, txNum, block, offset, codec.byteSize(value),
                new ValueWriter((p, pos) -> codec.write(p, pos, value), String.valueOf(value)));
    }

    /* Writing to log (undo-redo)

     The UNDO-REDO logs will look like this
     <OPERATOR (int), txNum (long), filename (string), blockNum (int), oldValueOffset (int), oldValue (T), newValueOffset (int), newValue (T)>
     */
    public static <T> long writeToLog(LogMgr logMgr, int operator, PageCodec<T> codec, long txNum, BlockId block,
                                      int oldOffset, T oldValue, int newOffset, T newValue) {
        return LogRecord.writeToLog(logMgr, operator, txNum, block, oldOffset, newOffset,
                codec.byteSize(oldValue), codec.byteSize(newValue),
                new ValueWriter((p, pos) -> codec.write(p, pos, oldValue), String.valueOf(oldValue)),
                new ValueWriter((p, pos) -> codec.write(p, pos, newValue), String.valueOf(newValue)));
    }
}
