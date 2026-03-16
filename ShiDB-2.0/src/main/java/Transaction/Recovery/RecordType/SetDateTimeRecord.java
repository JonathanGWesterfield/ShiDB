package Transaction.Recovery.RecordType;

import File.BlockId;
import File.Page;
import Log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.TestOnly;
import Transaction.Transaction;
import Transaction.Recovery.LogRecord;

import java.time.LocalDateTime;

@Slf4j(topic = "RecoveryMgr")
public class SetDateTimeRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_DATETIME;

    @Getter
    private final boolean isDataRecord = true;

    private final SetValueRecord<LocalDateTime> inner;

    public SetDateTimeRecord(Page page) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.DATE_TIME, page);
    }

    @TestOnly
    public SetDateTimeRecord(Long txNum, BlockId block, Integer offset, LocalDateTime oldValue, LocalDateTime newValue) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.DATE_TIME, txNum, block, offset, oldValue, newValue);
    }

    @TestOnly
    public SetValueRecord<LocalDateTime> getInner() {
        return inner;
    }

    @Override
    public byte[] toBytes() {
        return inner.toBytes();
    }

    @Override
    public long getTxNum() {
        return inner.getTxNum();
    }

    @Override
    public void undo(Transaction tx) {
        inner.undo(tx);
    }

    @Override
    public void redo(Transaction tx) {
        inner.redo(tx);
    }

    @Override
    public String toString() {
        return inner.toString();
    }

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, LocalDateTime value) {
        return SetValueRecord.writeToLog(logMgr, LogRecord.SET_DATETIME, PageCodecs.DATE_TIME, txNum, block, offset, value);
    }

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, LocalDateTime oldValue,
                                  LocalDateTime newValue) {
        return SetValueRecord.writeToLog(logMgr, LogRecord.SET_DATETIME, PageCodecs.DATE_TIME, txNum, block, offset, oldValue,
                newValue);
    }
}

