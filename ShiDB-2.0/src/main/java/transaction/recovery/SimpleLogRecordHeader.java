package transaction.recovery;

import file.Page;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j(topic = "RecoveryMgr")
public class SimpleLogRecordHeader {

    @Getter
    private long txNum;

    public SimpleLogRecordHeader(Page page) {
        int operator = page.getInt(0);

        int txPosition = Integer.BYTES;
        txNum = page.getLong(txPosition);

        log.debug("Loading up log record: {}", recordToString(operator, txNum));
    }

    public static String recordToString(int operator, long txNum) {
        String strOp = LogRecord.operatorToString(operator);

        // Thought about using StringBuilder, but the compiler should be able to optimize a single line string concat
        return "<" + strOp + ", tx: " + txNum + ">";
    }
}
