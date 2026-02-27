package transaction.recovery;

import file.BlockId;
import file.Page;
import log.LogMgr;
import lombok.Getter;
import transaction.ShouldLog;
import transaction.Transaction;

public class SetStringRecord implements LogRecord {
    @Getter
    static final int operator = LogRecord.SET_STRING;

    @Getter
    private long txNum;

    @Getter
    private String value;

    @Getter
    private int offset;

    @Getter
    private BlockId block;

    public SetStringRecord(Page page) {
        int txPosition = Integer.BYTES;
        txNum = page.getLong(txPosition);

        int filenamePosition = txPosition + Long.BYTES;
        String filename = page.getString(filenamePosition);

        int blockNumPosition = filenamePosition + Page.calcMaxByteLength(filename);
        int blockNum = page.getInt(blockNumPosition);
        block = new BlockId(filename, blockNum);

        int offsetPosition = blockNumPosition + Integer.BYTES;
        offset = page.getInt(offsetPosition);

        int valuePosition = offsetPosition + Integer.BYTES;
        value = page.getString(valuePosition);
    }

    public String toString() {
        // Thought about using StringBuilder, but the compiler should be able to optimize a single line string concat
        return "<SET_STRING tx: " + txNum + ", block: " + block + ", offset: " + offset + ", value: " + value + ">";
    }

    public void undo(Transaction tx) {
        tx.pin(block);
        tx.setString(block, offset, value, ShouldLog.OK_TO_LOG);
        tx.unPin(block);
    }
    
    /* SET_STRING record is laid out as such:
        <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (string)>
    */
    public static void writeToLog(LogMgr logMgr, long txNum, BlockId block, int offset, String value) {
        int txPosition = Integer.BYTES;
        int filenamePosition = txPosition + Long.BYTES;
        int blockNumPosition = filenamePosition + Page.calcMaxByteLength(block.filename());
        int offsetPosition = blockNumPosition + Integer.BYTES;
        int valuePosition = offsetPosition + Integer.BYTES;

        int recordLength = valuePosition + Page.calcMaxByteLength(value);

        byte[] record = new byte[recordLength];
        Page page = new Page(record);

        page.setInt(0, getOperator());
        page.setLong(txPosition, txNum);
        page.setString(filenamePosition, block.filename());
        page.setInt(blockNumPosition, block.blockNum());
        page.setInt(offsetPosition, offset);
        page.setString(valuePosition, value);

        logMgr.appendRecord(record);
    }
}
