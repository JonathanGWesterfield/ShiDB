package transaction.recovery.recordtype;

import file.BlockId;
import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import transaction.Transaction;
import transaction.recovery.LogRecord;

@Slf4j(topic = "RecoveryMgr")
public class SetStringRecord implements LogRecord {
    @Getter
    private static final int operator = LogRecord.SET_STRING;

    private final SetValueRecord<String> inner;

    public SetStringRecord(Page page) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.STRING, page);
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

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, String value) {
        return SetValueRecord.writeToLog(logMgr, operator, PageCodecs.STRING, txNum, block, offset, value);
    }

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int oldOffset, String oldValue,
                                  int newOffset, String newValue) {
        return SetValueRecord.writeToLog(logMgr, operator, PageCodecs.STRING, txNum, block, oldOffset, oldValue,
                newOffset, newValue);
    }
}
