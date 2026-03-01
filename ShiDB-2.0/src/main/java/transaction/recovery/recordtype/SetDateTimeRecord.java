package transaction.recovery.recordtype;

import file.BlockId;
import file.Page;
import log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import transaction.ShouldLog;
import transaction.Transaction;
import transaction.recovery.DataLogRecordHeader;
import transaction.recovery.LogRecord;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
public class SetDateTimeRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_DATETIME;

    @Getter
    private long txNum;

    @Getter
    private LocalDateTime value;

    @Getter
    private int offset;

    @Getter
    private BlockId block;

    public SetDateTimeRecord(Page page) {
        DataLogRecordHeader header = new DataLogRecordHeader(page);
        this.txNum = header.getTxNum();
        this.block = header.getBlock();
        this.offset = header.getOffset();

        value = page.getDateTime(header.getValuePosition());
    }

    public void undo(Transaction tx) {
        tx.pin(block);
        tx.setDateTime(block, offset, value, ShouldLog.DO_NOT_LOG); // don't log the undo!
        tx.unPin(block);
    }

    public String toString() {
        return "<SET_DATETIME tx: " + txNum + ", block: " + block + ", offset: " + offset + ", value: " + value + ">";
    }

    /* SET_DATETIME record is laid out as such:
        <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (long)>
    */
    public static void writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, LocalDateTime value) {
        long epoch = value.toEpochSecond(ZoneOffset.UTC);

        log.debug("Writing {} log record. TxNum: {}, filename: {}, Block Num: {}, offset: {}, value: {}",
                LogRecord.operatorToString(LogRecord.SET_DATETIME), txNum, block.filename(), block.blockNum(), offset, value);
        LogRecord.writeToLog(logMgr, LogRecord.SET_DATETIME, txNum, block, offset, Long.BYTES,
                (page, position) -> page.setLong(position, epoch));
    }
}

