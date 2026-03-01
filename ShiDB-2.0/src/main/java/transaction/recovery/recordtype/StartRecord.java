package transaction.recovery.recordtype;

import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import transaction.Transaction;
import transaction.recovery.LogRecord;
import transaction.recovery.SimpleLogRecordHeader;

@Slf4j(topic = "RecoveryMgr")
public class StartRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.START;

    @Getter
    private long txNum;

    public StartRecord(Page page) {
        SimpleLogRecordHeader header = new SimpleLogRecordHeader(page);
        txNum = header.getTxNum();
    }

    // Does nothing, because a start record contains no undo information.
    public void undo(Transaction tx) {}

    public static void writeToLog(LogMgr logMgr, long txNum) {
        log.debug("Writing {} log record. TxNum: {}", LogRecord.operatorToString(LogRecord.START), txNum);
        LogRecord.writeToLog(logMgr, LogRecord.START, txNum);
    }
}
