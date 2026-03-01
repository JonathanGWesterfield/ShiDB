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
public class SetByteRecord implements LogRecord {
    @Getter
    private final int operator = LogRecord.SET_BYTE;

    @Getter
    private long txNum;

    @Getter
    private byte value;

    @Getter
    private int offset;

    @Getter
    private BlockId block;

    public SetByteRecord(Page page) {
        DataLogRecordHeader header = new DataLogRecordHeader(page);
        this.txNum = header.getTxNum();
        this.block = header.getBlock();
        this.offset = header.getOffset();

        value = page.getByte(header.getValuePosition());
    }

    public void undo(Transaction tx) {
        tx.pin(block);
        tx.setByte(block, offset, value, ShouldLog.DO_NOT_LOG); // don't log the undo!
        tx.unPin(block);
    }

    public String toString() {
        return "<SET_BYTE  tx: " + txNum + ", block: " + block + ", offset: " + offset + ", value: " + value + ">";
    }

    /* SET_BYTE record is laid out as such:
        <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (byte)>
    */
    public static long writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, byte value) {
        log.debug("Writing {} log record. TxNum: {}, filename: {}, Block Num: {}, offset: {}, value: {}",
                LogRecord.operatorToString(LogRecord.SET_BYTE), txNum, block.filename(), block.blockNum(), offset, value);
        return LogRecord.writeToLog(logMgr, LogRecord.SET_BYTE, txNum, block, offset, Byte.BYTES,
                (page, position) -> page.setByte(position, value));
    }
}

