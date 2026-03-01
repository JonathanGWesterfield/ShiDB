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

    public static String recordToString(int operator, long txNum) {
        String strOp = LogRecord.operatorToString(operator);

        // Thought about using StringBuilder, but the compiler should be able to optimize a single line string concat
        return "<" + strOp + ", tx: " + txNum + ">";
    }
}
