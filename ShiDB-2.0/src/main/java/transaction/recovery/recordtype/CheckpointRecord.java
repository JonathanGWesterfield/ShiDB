package transaction.recovery.recordtype;

import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.TestOnly;
import transaction.Transaction;
import transaction.recovery.LogRecord;
import transaction.recovery.SimpleLogRecordHeader;

import java.util.Optional;

@Getter
@Slf4j(topic = "RecoveryMgr")
public class CheckpointRecord implements LogRecord {
    private final int operator = LogRecord.CHECKPOINT;

    private final boolean isDataRecord = false;

    private long txNum;

    public CheckpointRecord(Page page) {
        SimpleLogRecordHeader header = new SimpleLogRecordHeader(page);
        txNum = header.getTxNum();
    }

    @TestOnly
    public CheckpointRecord(long txNum) {
        this.txNum = txNum;
    }

    @Override
    public byte[] toBytes() {
        return LogRecord.toBytes(operator, txNum, Optional.empty());
    }

    @Override
    public String toString() {
        return SimpleLogRecordHeader.recordToString(operator, txNum);
    }

    // Does nothing, because a checkpoint record contains no undo information.
    @Override
    public void undo(Transaction tx) {}

    // Does nothing, because a checkpoint record contains no redo information.
    @Override
    public void redo(Transaction tx) {}

    public static long writeToLog(LogMgr logMgr, long txNum) {
        return LogRecord.writeToLog(logMgr, LogRecord.CHECKPOINT, txNum, Optional.empty());
    }
}
