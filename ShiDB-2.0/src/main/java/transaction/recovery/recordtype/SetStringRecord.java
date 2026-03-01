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
public class SetStringRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_STRING;

    @Getter
    private long txNum;

    @Getter
    private String value;

    @Getter
    private int offset;

    @Getter
    private BlockId block;

    public SetStringRecord(Page page) {
        DataLogRecordHeader header = new DataLogRecordHeader(page);
        this.txNum = header.getTxNum();
        this.block = header.getBlock();
        this.offset = header.getOffset();

        value = page.getString(header.getValuePosition());
    }

    public String toString() {
        // Thought about using StringBuilder, but the compiler should be able to optimize a single line string concat
        return "<SET_STRING tx: " + txNum + ", block: " + block + ", offset: " + offset + ", value: " + value + ">";
    }

    public void undo(Transaction tx) {
        tx.pin(block);
        tx.setString(block, offset, value, ShouldLog.DO_NOT_LOG); // don't log the undo!
        tx.unPin(block);
    }

    /* SET_STRING record is laid out as such:
        <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (string)>
    */
    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, String value) {
        log.debug("Writing {} log record. TxNum: {}, filename: {}, Block Num: {}, offset: {}, value: {}",
                LogRecord.operatorToString(LogRecord.SET_STRING), txNum, block.filename(), block.blockNum(), offset, value);
        return LogRecord.writeToLog(logMgr, LogRecord.SET_STRING, txNum, block, offset, Page.calcMaxByteLength(value),
                (page, position) -> page.setString(position, value));
    }
}
