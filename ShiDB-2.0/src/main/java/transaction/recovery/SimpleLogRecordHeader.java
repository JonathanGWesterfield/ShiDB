package transaction.recovery;

import file.Page;
import lombok.Getter;

public class SimpleLogRecordHeader {

    @Getter
    private long txNum;

    public SimpleLogRecordHeader(Page page) {
        // The operator is before the txNum and is and Integer, so we start with the transaction num

        int txPosition = Integer.BYTES;
        txNum = page.getLong(txPosition);
    }
}
