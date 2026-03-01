package transaction.recovery.recordtype;

import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import transaction.Transaction;
import transaction.recovery.LogRecord;
import transaction.recovery.SimpleLogRecordHeader;

@Slf4j(topic = "RecoveryMgr")
public class CheckpointRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.CHECKPOINT;

    @Getter
    private long txNum;

    public CheckpointRecord(Page page) {
        SimpleLogRecordHeader header = new SimpleLogRecordHeader(page);
        txNum = header.getTxNum();
    }

    // Private constructor to help with logging
    private CheckpointRecord(long txNum) {
        this.txNum = txNum;
    }

    public String toString() {
        return SimpleLogRecordHeader.recordToString(operator, txNum);
    }

    // Does nothing, because a checkpoint record contains no undo information.
    public void undo(Transaction tx) {}

    public static long writeToLog(LogMgr logMgr, long txNum) {
        CheckpointRecord record = new CheckpointRecord(txNum);

        log.debug("Writing log record: {}", record);
        return LogRecord.writeToLog(logMgr, LogRecord.CHECKPOINT, txNum);
    }
}
