package transaction.recovery.recordtype;

import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import transaction.Transaction;
import transaction.recovery.LogRecord;
import transaction.recovery.SimpleLogRecordHeader;

@Slf4j(topic = "RecoveryMgr")
public class RollbackRecord implements LogRecord {

    @Getter
    private final int operator = LogRecord.ROLLBACK;

    @Getter
    private long txNum;

    public RollbackRecord(Page page) {
        SimpleLogRecordHeader header = new SimpleLogRecordHeader(page);
        txNum = header.getTxNum();
    }

    // Does nothing, because a rollback record contains no undo information.
    public void undo(Transaction tx) {}

    public static long writeToLog(LogMgr logMgr, long txNum) {
        log.debug("Writing {} log record. TxNum: {}", LogRecord.operatorToString(LogRecord.ROLLBACK), txNum);
        return LogRecord.writeToLog(logMgr, LogRecord.ROLLBACK, txNum);
    }
}
