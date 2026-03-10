package transaction.recovery.recordtype;

import file.BlockId;
import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.TestOnly;
import transaction.Transaction;
import transaction.recovery.LogRecord;

@Slf4j(topic = "RecoveryMgr")
public class SetLongRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_LONG;

    @Getter
    private final boolean isDataRecord = true;

    private final SetValueRecord<Long> inner;

    public SetLongRecord(Page page) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.LONG, page);
    }

    @TestOnly
    public SetLongRecord(Long txNum, BlockId block, Integer offset, Long oldValue, Long newValue) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.LONG, txNum, block, offset, oldValue, newValue);
    }

    @TestOnly
    public SetValueRecord<Long> getInner() {
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

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, long value) {
        return SetValueRecord.writeToLog(logMgr, LogRecord.SET_LONG, PageCodecs.LONG, txNum, block, offset, value);
    }

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, long oldValue, long newValue) {
        return SetValueRecord.writeToLog(logMgr, LogRecord.SET_LONG, PageCodecs.LONG, txNum, block, offset, oldValue,
                newValue);
    }
}
