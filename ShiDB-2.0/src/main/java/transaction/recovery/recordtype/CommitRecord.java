package transaction.recovery.recordtype;

import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import transaction.Transaction;
import transaction.recovery.LogRecord;
import transaction.recovery.SimpleLogRecordHeader;

@Slf4j(topic = "RecoveryMgr")
public class CommitRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.COMMIT;

    @Getter
    private long txNum;

    public CommitRecord(Page page) {
        SimpleLogRecordHeader header = new SimpleLogRecordHeader(page);
        txNum = header.getTxNum();
    }

    public String toString() {
        return SimpleLogRecordHeader.recordToString(operator, txNum);
    }

    // Does nothing, because a commit record contains no undo information.
    public void undo(Transaction tx) {}

    // Does nothing, because a commit record contains no redo information.
    public void redo(Transaction tx) {}

    public static long writeToLog(LogMgr logMgr, long txNum) {
        return LogRecord.writeToLog(logMgr, LogRecord.COMMIT, txNum);
    }
}
