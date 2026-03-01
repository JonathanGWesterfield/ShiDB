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

@Slf4j(topic = "RecoveryMgr")
public class SetBooleanRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_SHORT;

    @Getter
    private long txNum;

    @Getter
    private boolean value;

    @Getter
    private int offset;

    @Getter
    private BlockId block;

    public SetBooleanRecord(Page page) {
        DataLogRecordHeader header = new DataLogRecordHeader(page);
        this.txNum = header.getTxNum();
        this.block = header.getBlock();
        this.offset = header.getOffset();

        value = page.getBoolean(header.getValuePosition());
    }

    public void undo(Transaction tx) {
        tx.pin(block);
        tx.setBoolean(block, offset, value, ShouldLog.DO_NOT_LOG); // don't log the undo!
        tx.unPin(block);
    }

    public String toString() {
        return "<SET_BOOLEAN  tx: " + txNum + ", block: " + block + ", offset: " + offset + ", value: " + value + ">";
    }

    /* SET_BOOLEAN record is laid out as such:
        <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (boolean)>
    */
    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, boolean value) {
        log.debug("Writing {} log record. TxNum: {}, filename: {}, Block Num: {}, offset: {}, value: {}",
                LogRecord.operatorToString(LogRecord.SET_BOOLEAN), txNum, block.filename(), block.blockNum(), offset, value);
        return LogRecord.writeToLog(logMgr, LogRecord.SET_BOOLEAN, txNum, block, offset, Byte.BYTES,
                (page, position) -> page.setBoolean(position, value));
    }
}



