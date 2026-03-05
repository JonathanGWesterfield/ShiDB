package transaction.recovery.recordtype;

import file.BlockId;
import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import transaction.Transaction;
import transaction.recovery.LogRecord;

@Slf4j(topic = "RecoveryMgr")
public class SetDoubleRecord implements LogRecord {
    @Getter
    private static final int operator = LogRecord.SET_DOUBLE;

    private final SetValueRecord<Double> inner;

    public SetDoubleRecord(Page page) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.DOUBLE, page);
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

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, double value) {
        return SetValueRecord.writeToLog(logMgr, operator, PageCodecs.DOUBLE, txNum, block, offset, value);
    }

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int oldOffset, double oldValue, int newOffset,
                                  double newValue) {
        return SetValueRecord.writeToLog(logMgr, operator, PageCodecs.DOUBLE, txNum, block, oldOffset, oldValue,
                newOffset, newValue);
    }
}

