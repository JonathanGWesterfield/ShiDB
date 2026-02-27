package transaction.recovery;

import transaction.Transaction;

/**
 * All complex log records like setInt and setString should be laid out within their byte buffers like so:
 * <OPERATOR (int), txNum (long), filename (string), blockNum (int), offset (int), value (depends on the record type)>
 *
 * All simple records like commit and rollback should be laid out like so:
 * <OPERATOR (int), txNum (long)>
 */

public interface LogRecord {

    // TODO: Since I implemented the ability to write longs, bytes, doubles and other primitives, those will need to
    // be added at some point too along with their corresponding record classes
    public static final int CHECKPOINT = 0, START = 1, COMMIT = 2, ROLLBACK = 3, SET_INT = 4, SET_STRING = 5,
            SET_BYTE = 6, SET_SHORT = 7, SET_LONG = 8, SET_DOUBLE = 9, SET_DATETIME = 10;

    int getOperator();
    long getTxNum();
    void undo(Transaction tx);
    String toString();
}
