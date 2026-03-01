package transaction.recovery;

import file.BlockId;
import file.Page;
import lombok.Getter;

/**
 * All complex/data log records like setInt and setString should be laid out within their byte buffers like so:
 *  * <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (depends on the record type)>
 */
public class DataLogRecordHeader {
    @Getter
    private long txNum;

    @Getter
    private BlockId block;

    @Getter
    private int offset;

    @Getter
    private int valuePosition;

    public DataLogRecordHeader(Page page) {
        int txPosition = Integer.BYTES;
        this.txNum = page.getLong(txPosition);

        int filenamePosition = txPosition + Long.BYTES;
        String filename = page.getString(filenamePosition);

        int blockPosition = filenamePosition + Page.calcMaxByteLength(filename);
        int blockNum = page.getInt(blockPosition);
        this.block = new BlockId(filename, blockNum);

        int offsetPosition = blockPosition + Integer.BYTES;
        this.offset = page.getInt(offsetPosition);

        this.valuePosition = offsetPosition + Integer.BYTES;
    }
}
