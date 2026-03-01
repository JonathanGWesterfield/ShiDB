package transaction.recovery;

import file.BlockId;
import file.Page;
import log.LogMgr;
import transaction.Transaction;

/**
 * All complex log records like setInt and setString should be laid out within their byte buffers like so:
 * <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (depends on the record type)>
 *
 * All simple records like commit and rollback should be laid out like so:
 * <OPERATOR (int), txNum (long)>
 */
public interface LogRecord {
    public static final int CHECKPOINT = 0, START = 1, COMMIT = 2, ROLLBACK = 3, SET_INT = 4, SET_STRING = 5,
            SET_BYTE = 6, SET_SHORT = 7, SET_LONG = 8, SET_DOUBLE = 9, SET_DATETIME = 10, SET_BOOLEAN = 11;

    static String operatorToString(int operator) {
        return switch(operator) {
            case LogRecord.CHECKPOINT -> "CHECKPOINT";
            case LogRecord.START -> "START";
            case LogRecord.COMMIT -> "COMMIT";
            case LogRecord.SET_INT -> "SET_INT";
            case LogRecord.SET_STRING -> "SET_STRING";
            case LogRecord.SET_BYTE -> "SET_BYTE";
            case LogRecord.SET_SHORT -> "SET_SHORT";
            case LogRecord.SET_LONG -> "SET_LONG";
            case LogRecord.SET_DOUBLE -> "SET_DOUBLE";
            case LogRecord.SET_DATETIME -> "SET_DATETIME";
            case LogRecord.SET_BOOLEAN -> "SET_BOOLEAN";
            default -> throw new RuntimeException("Encountered an unsupported operator: " + operator);
        };
    }

    @FunctionalInterface
    interface ValueWriter {
        // This is just a fancy way of saying "pass an anonymous lambda in with this function signature so that we can
        // call the correct page.set<data type>() function"
        void write(Page page, int position);
    }

    static long writeToLog(LogMgr logMgr, int operator, long txNum, BlockId block, int offset, int valueByteSize,
                           ValueWriter valueWriter) {
        int txPosition = Integer.BYTES;
        int filenamePosition = txPosition + Long.BYTES;
        int blockNumPosition = filenamePosition + Page.calcMaxByteLength(block.filename());
        int offsetPosition = blockNumPosition + Integer.BYTES;
        int valuePosition = offsetPosition + Integer.BYTES;

        int recordLength = valuePosition + valueByteSize;

        byte[] record = new byte[recordLength];
        Page page = new Page(record);

        page.setInt(0, operator);
        page.setLong(txPosition, txNum);
        page.setString(filenamePosition, block.filename());
        page.setInt(blockNumPosition, block.blockNum());
        page.setInt(offsetPosition, offset);

        valueWriter.write(page, valuePosition);

        return logMgr.appendRecord(record);
    }

    static long writeToLog(LogMgr logMgr, int operator, long txNum) {
        int txPosition = Integer.BYTES;

        int recordLength = txPosition + Long.BYTES;

        byte[] record = new byte[recordLength];
        Page page = new Page(record);

        page.setInt(0, operator);
        page.setLong(txPosition, txNum);

        return logMgr.appendRecord(record);
    }

    int getOperator();
    long getTxNum();
    void undo(Transaction tx);
    String toString();
}
