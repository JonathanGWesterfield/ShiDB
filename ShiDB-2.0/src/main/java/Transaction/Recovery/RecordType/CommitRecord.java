package Transaction.Recovery.RecordType;

import File.Page;
import Log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.TestOnly;
import Transaction.Transaction;
import Transaction.Recovery.LogRecord;
import Transaction.Recovery.SimpleLogRecordHeader;

import java.util.Optional;

@Getter
@Slf4j(topic = "RecoveryMgr")
public class CommitRecord implements LogRecord {
    private final int operator = LogRecord.COMMIT;

    private final boolean isDataRecord = false;

    private long txNum;

    public CommitRecord(Page page) {
        SimpleLogRecordHeader header = new SimpleLogRecordHeader(page);
        txNum = header.getTxNum();
    }

    @TestOnly
    public CommitRecord(long txNum) {
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

    // Does nothing, because a commit record contains no undo information.
    @Override
    public void undo(Transaction tx) {}

    // Does nothing, because a commit record contains no redo information.
    @Override
    public void redo(Transaction tx) {}

    public static long writeToLog(LogMgr logMgr, long txNum) {
        return LogRecord.writeToLog(logMgr, LogRecord.COMMIT, txNum, Optional.empty());
    }
}
