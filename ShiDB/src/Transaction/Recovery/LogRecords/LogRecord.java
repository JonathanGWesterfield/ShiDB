package Transaction.Recovery.LogRecords;

import File.Page;
import Transaction.Transaction;

/**
 * Interface for the various log records. Generally the structure of a log record will
 * be as shown:
 *
 * --LogOp, TransactionNum, filename, block number, offset, value--
 *
 * "SET" records will have all of the above values, while others will have less. Refer
 * to the implementing class to determine the specific record structure that class uses.
 *
 * Log operators are implemented as Bytes instead of integers for space savings.
 */
public interface LogRecord {
    // These are logOps -> log operators
    public static final byte CHECKPOINT = 0;
    public static final byte START = 1;
    public static final byte COMMIT = 2;
    public static final byte ROLLBACK = 3;
    public static final byte SETINT = 4;
    public static final byte SETSTRING = 5;
    public static final byte SETLONG = 6;
    public static final byte SETBOOLEAN = 7;
    public static final byte SETDATETIME = 8;
    public static final byte SETBYTE = 9;
    public static final byte NQCHECKPOINT = 10;

    /**
     * Gets the records operator
     * @return The operator of the log record (START, COMMIT, ..., etc.)
     */
    int getOperator();

    /**
     * Gets the transaction number of the transaction that wrote the log record. CHECKPOINT
     * records return a dummy number since a transaction wouldn't explicitly create that
     * @return The transaction number of the log record
     */
    int getTxNumber();

    /**
     * Restores any changes in that record. Only "SET" operations will implement this function.
     * The "SET" records will pin a buffer to the specified block, write the specified
     * value at the specified offset, and unpin the buffer
     * @param tx The transaction we want to undo
     */
    void undo(Transaction tx);

    /**
     * Factory method that generates the proper LogRecord object based on the log operator
     * pulled from the beginning of the byte array.
     * @param bytes
     * @return
     */
    static LogRecord createLogRecord(byte[] bytes) {
        Page page = new Page(bytes);
        switch(page.getByte(0)) {
        case CHECKPOINT:
        case NQCHECKPOINT:
        case START:
        case COMMIT:
        case ROLLBACK:
        case SETINT:
        case SETSTRING:
        case SETLONG:
        case SETBOOLEAN:
        case SETDATETIME:
        case SETBYTE:
        default:
            return null;
        }
    }
}
