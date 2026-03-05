package transaction.recovery.recordtype;

import file.BlockId;
import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import transaction.ShouldLog;
import transaction.Transaction;
import transaction.recovery.DataLogRecordHeader;
import transaction.recovery.LogRecord;

@Slf4j(topic = "RecoveryMgr")
public class SetBooleanRecord implements LogRecord {
    @Getter
    private static final int operator = LogRecord.SET_BOOLEAN;

    private final SetValueRecord<Boolean> inner;

    public SetBooleanRecord(Page page) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.BOOLEAN, page);
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
    public String toString() {
        return inner.toString();
    }

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, boolean value) {
        return SetValueRecord.writeToLog(logMgr, operator, PageCodecs.BOOLEAN, txNum, block, offset, value);
    }

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int oldOffset,
                                  boolean oldValue, int newOffset, boolean newValue) {
        return SetValueRecord.writeToLog(logMgr, operator, PageCodecs.BOOLEAN, txNum, block, oldOffset, oldValue,
                newOffset, newValue);
    }
}



