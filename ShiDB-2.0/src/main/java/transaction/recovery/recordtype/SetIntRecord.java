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
public class SetIntRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_INT;

    @Getter
    private long txNum;

    @Getter
    private int value;

    @Getter
    private int offset;

    @Getter
    private BlockId block;

    public SetIntRecord(Page page) {
        DataLogRecordHeader header = new DataLogRecordHeader(page);
        this.txNum = header.getTxNum();
        this.block = header.getBlock();
        this.offset = header.getOffset();

        value = page.getInt(header.getValuePosition());
    }

    public String toString() {
        // Thought about using StringBuilder, but the compiler should be able to optimize a single line string concat
        return "<SET_INT tx: " + txNum + ", block: " + block + ", offset: " + offset + ", value: " + value + ">";
    }

    public void undo(Transaction tx) {
        tx.pin(block);
        tx.setInt(block, offset, value, ShouldLog.DO_NOT_LOG); // don't log the undo!
        tx.unPin(block);
    }

    /* SET_INT record is laid out as such:
        <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (int)>
    */
    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, int value) {
        log.debug("Writing {} log record. TxNum: {}, filename: {}, Block Num: {}, offset: {}, value: {}",
                LogRecord.operatorToString(LogRecord.SET_INT), txNum, block.filename(), block.blockNum(), offset, value);
        return LogRecord.writeToLog(logMgr, LogRecord.SET_INT, txNum, block, offset, Integer.BYTES,
                (page, position) -> page.setInt(position, value));
    }
}
