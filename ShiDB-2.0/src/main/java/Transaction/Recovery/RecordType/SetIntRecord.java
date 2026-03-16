package Transaction.Recovery.RecordType;

import File.BlockId;
import File.Page;
import Log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.TestOnly;
import Transaction.Transaction;
import Transaction.Recovery.LogRecord;

@Slf4j(topic = "RecoveryMgr")
public class SetIntRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_INT;

    @Getter
    private final boolean isDataRecord = true;

    private final SetValueRecord<Integer> inner;

    public SetIntRecord(Page page) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.INT, page);
    }

    @TestOnly
    public SetIntRecord(Long txNum, BlockId block, Integer offset, Integer oldValue, Integer newValue) {
        this.inner = new SetValueRecord<>(operator, PageCodecs.INT, txNum, block, offset, oldValue, newValue);
    }

    @TestOnly
    public SetValueRecord<Integer> getInner() {
        return inner;
    }

    @Override
    public byte[] toBytes() {
        return inner.toBytes();
    }

    @Override
    public long getTxNum()   {
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

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, int value) {
        return SetValueRecord.writeToLog(logMgr, LogRecord.SET_INT, PageCodecs.INT, txNum, block, offset, value);
    }

    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, int oldValue, int newValue) {
        return SetValueRecord.writeToLog(logMgr, LogRecord.SET_INT, PageCodecs.INT, txNum, block, offset, oldValue,
                newValue);
    }
}
