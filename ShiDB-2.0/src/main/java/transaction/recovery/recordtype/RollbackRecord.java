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
public class RollbackRecord implements LogRecord {

    private final int operator = LogRecord.ROLLBACK;

    private final boolean isDataRecord = false;

    private long txNum;

    public RollbackRecord(Page page) {
        SimpleLogRecordHeader header = new SimpleLogRecordHeader(page);
        txNum = header.getTxNum();
    }

    @TestOnly
    public RollbackRecord(long txNum) {
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

    // Does nothing, because a rollback record contains no undo information.
    @Override
    public void undo(Transaction tx) {}

    // Does nothing, because a rollback record contains no redo information.
    @Override
    public void redo(Transaction tx) {}

    public static long writeToLog(LogMgr logMgr, long txNum) {
        return LogRecord.writeToLog(logMgr, LogRecord.ROLLBACK, txNum, Optional.empty());
    }
}
