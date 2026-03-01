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
public class SetDoubleRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_DOUBLE;

    @Getter
    private long txNum;

    @Getter
    private double value;

    @Getter
    private int offset;

    @Getter
    private BlockId block;

    public SetDoubleRecord(Page page) {
        DataLogRecordHeader header = new DataLogRecordHeader(page);
        this.txNum = header.getTxNum();
        this.block = header.getBlock();
        this.offset = header.getOffset();

        value = page.getDouble(header.getValuePosition());
    }

    public void undo(Transaction tx) {
        tx.pin(block);
        tx.setDouble(block, offset, value, ShouldLog.DO_NOT_LOG); // don't log the undo!
        tx.unPin(block);
    }

    public String toString() {
        return "<SET_DOUBLE  tx: " + txNum + ", block: " + block + ", offset: " + offset + ", value: " + value + ">";
    }

    /* SET_DOUBLE record is laid out as such:
        <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (double)>
    */
    public static void writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, double value) {
        log.debug("Writing {} log record. TxNum: {}, filename: {}, Block Num: {}, offset: {}, value: {}",
                LogRecord.operatorToString(LogRecord.SET_DOUBLE), txNum, block.filename(), block.blockNum(), offset, value);
        LogRecord.writeToLog(logMgr, LogRecord.SET_DOUBLE, txNum, block, offset, Double.BYTES,
                (page, position) -> page.setDouble(position, value));
    }
}

