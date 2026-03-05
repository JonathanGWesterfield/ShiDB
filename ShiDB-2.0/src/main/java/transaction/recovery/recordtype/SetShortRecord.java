package transaction.recovery.recordtype;

import file.BlockId;
import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import transaction.Transaction;
import transaction.recovery.LogRecord;

@Slf4j(topic = "RecoveryMgr")
public class SetShortRecord implements LogRecord {
    @Getter
    private static final int operator = LogRecord.SET_SHORT;

    private final SetValueRecord<Short> inner;

    public SetShortRecord(Page page) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.SHORT, page);
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

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, short value) {
        return SetValueRecord.writeToLog(logMgr, operator, PageCodecs.SHORT, txNum, block, offset, value);
    }

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int oldOffset, short oldValue, int newOffset,
                                  short newValue) {
        return SetValueRecord.writeToLog(logMgr, operator, PageCodecs.SHORT, txNum, block, oldOffset, oldValue,
                newOffset, newValue);
    }
}


