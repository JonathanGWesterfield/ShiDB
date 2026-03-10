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
public class SetShortRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_SHORT;

    @Getter
    private final boolean isDataRecord = true;

    private final SetValueRecord<Short> inner;

    public SetShortRecord(Page page) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.SHORT, page);
    }

    @TestOnly
    public SetShortRecord(Long txNum, BlockId block, Integer offset, Short oldValue, Short newValue) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.SHORT, txNum, block, offset, oldValue, newValue);
    }

    @TestOnly
    public SetValueRecord<Short> getInner() {
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

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, short value) {
        return SetValueRecord.writeToLog(logMgr, LogRecord.SET_SHORT, PageCodecs.SHORT, txNum, block, offset, value);
    }

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, short oldValue, short newValue) {
        return SetValueRecord.writeToLog(logMgr, LogRecord.SET_SHORT, PageCodecs.SHORT, txNum, block, offset, oldValue,
                newValue);
    }
}


