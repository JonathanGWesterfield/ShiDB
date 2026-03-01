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

@Slf4j
public class SetLongRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_LONG;

    @Getter
    private long txNum;

    @Getter
    private long value;

    @Getter
    private int offset;

    @Getter
    private BlockId block;

    public SetLongRecord(Page page) {
        DataLogRecordHeader header = new DataLogRecordHeader(page);
        this.txNum = header.getTxNum();
        this.block = header.getBlock();
        this.offset = header.getOffset();

        value = page.getLong(header.getValuePosition());
    }

    public void undo(Transaction tx) {
        tx.pin(block);
        tx.setLong(block, offset, value, ShouldLog.DO_NOT_LOG); // don't log the undo!
        tx.unPin(block);
    }

    public String toString() {
        return "<SET_LONG tx: " + txNum + ", block: " + block + ", offset: " + offset + ", value: " + value + ">";
    }

    /* SET_LONG record is laid out as such:
        <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (long)>
    */
    public static void writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, long value) {
        log.debug("Writing {} log record. TxNum: {}, filename: {}, Block Num: {}, offset: {}, value: {}",
                LogRecord.operatorToString(LogRecord.SET_LONG), txNum, block.filename(), block.blockNum(), offset, value);
        LogRecord.writeToLog(logMgr, LogRecord.SET_LONG, txNum, block, offset, Long.BYTES,
                (page, position) -> page.setLong(position, value));
    }
}
