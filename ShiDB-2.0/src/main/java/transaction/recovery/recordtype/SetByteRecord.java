package transaction.recovery.recordtype;

import file.BlockId;
import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import transaction.Transaction;
import transaction.recovery.LogRecord;

@Slf4j(topic = "RecoveryMgr")
public class SetByteRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_BYTE;

    private final SetValueRecord<Byte> inner;

    public SetByteRecord(Page page) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.BYTE, page);
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

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, byte value) {
        return SetValueRecord.writeToLog(logMgr, LogRecord.SET_BYTE, PageCodecs.BYTE, txNum, block, offset, value);
    }

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int oldOffset, byte oldValue, int newOffset,
                                  byte newValue) {
        return SetValueRecord.writeToLog(logMgr, LogRecord.SET_BYTE, PageCodecs.BYTE, txNum, block, oldOffset, oldValue,
                newOffset, newValue);
    }
}

