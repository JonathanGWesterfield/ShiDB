package Transaction.Recovery;

import File.BlockId;
import File.Page;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import Server.ConfigFetcher;

/**
 * All complex/data log records like setInt and setString should be laid out within their byte buffers like so:
 *  * <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (depends on the record type)>
 *
 *    <OPERATOR (int), txNum (long), filename (string), blockNum (int), oldValueOffset (int), oldValue (depends on the record type), newValueOffset (int), newValue (depends on the record type)>
 */
@Slf4j(topic = "RecoveryMgr")
public class DataLogRecordHeader {
    @Getter
    private long txNum;

    @Getter
    private BlockId block;

    @Getter
    private int operator;

    @Getter
    private final int valueAreaStart; // position right after blockNum

    public DataLogRecordHeader(Page page) {
        this.operator = page.getInt(0);
        log.debug("DataLogRecordHeader operator: {} -> {}", operator, LogRecord.operatorToString(operator));

        int txPosition = Integer.BYTES;
        this.txNum = page.getLong(txPosition);

        int filenamePosition = txPosition + Long.BYTES;
        String filename = page.getString(filenamePosition);

        int blockPosition = filenamePosition + Page.calcMaxByteLength(filename);
        int blockNum = page.getInt(blockPosition);
        this.block = new BlockId(filename, blockNum);

        this.valueAreaStart = blockPosition + Integer.BYTES;

        log.debug("operator pos: 0, txPos: {}, filenamePos: {}, filename: '{}', blockPos: {}, valueAreaStart: {}",
                txPosition, filenamePosition, filename, blockPosition, valueAreaStart);
    }

    public static String recordToString(int operator, long txNum, BlockId block, int oldValueOffset, int newValueOffset,
                                        Object oldValue, Object newValue) {
        String strOp = LogRecord.operatorToString(operator);

        if (ConfigFetcher.getRecoveryMgrStrategy() == RecoveryMgrStrategy.UNDO_ONLY) {
            // Thought about using StringBuilder, but the compiler should be able to optimize a single line string concat
            return "<" + strOp + ", tx: " + txNum + ", block: " + block + ", offset: " + oldValueOffset +
                    ", oldValue: " + oldValue + ">";
        }

        return "<" + strOp + ", tx: " + txNum + ", block: " + block + ", oldValueOffset: " + oldValueOffset +
                ", oldValue: " + oldValue + ", newValueOffset: " + newValueOffset + ", newValue: " + newValue + ">";
    }
}
