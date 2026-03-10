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
public class SetStringRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_STRING;

    @Getter
    private final boolean isDataRecord = true;

    private final SetValueRecord<String> inner;

    public SetStringRecord(Page page) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.STRING, page);
    }

    @TestOnly
    public SetStringRecord(Long txNum, BlockId block, Integer offset, String oldValue, String newValue) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.STRING, txNum, block, offset, oldValue, newValue);
    }

    @TestOnly
    public SetValueRecord<String> getInner() {
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

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, String value) {
        return SetValueRecord.writeToLog(logMgr, LogRecord.SET_STRING, PageCodecs.STRING, txNum, block, offset, value);
    }

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, String oldValue, String newValue) {
        return SetValueRecord.writeToLog(logMgr, LogRecord.SET_STRING, PageCodecs.STRING, txNum, block, offset, oldValue,
                newValue);
    }
}
