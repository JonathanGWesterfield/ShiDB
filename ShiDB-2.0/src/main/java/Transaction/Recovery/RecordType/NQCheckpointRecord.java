package Transaction.Recovery.RecordType;

import File.Page;
import Log.LogMgr;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.TestOnly;
import Transaction.Transaction;
import Transaction.Recovery.LogRecord;
import Transaction.Recovery.SimpleLogRecordHeader;

import java.util.ArrayList;
import java.util.Optional;

@Getter
@Slf4j(topic = "RecoveryMgr")
public class NQCheckpointRecord implements LogRecord {
    private final int operator = LogRecord.NQ_CHECKPOINT;

    private final boolean isDataRecord = false;

    private long txNum;

    private ArrayList<Long> runningTxNums;

    /*
    NQ Checkpoint is laid like so:
    <operator (int), txNum (long), runningTxListLength (int), txNum1 (long), txNum2 (long), ... txNumN (long) >
     */
    public NQCheckpointRecord(Page page) {
        SimpleLogRecordHeader header = new SimpleLogRecordHeader(page);
        txNum = header.getTxNum();

        int runningTxLengthPosition = Integer.BYTES + Long.BYTES;
        int numRunningTransactions = page.getInt(runningTxLengthPosition);
        int txNumListOffset = runningTxLengthPosition + Integer.BYTES;

        this.runningTxNums = new ArrayList<>();

        for (int i = 0; i < numRunningTransactions; i++) {
            runningTxNums.add(page.getLong(txNumListOffset));
            txNumListOffset += Long.BYTES;
        }
    }

    @TestOnly
    public NQCheckpointRecord(long txNum, ArrayList<Long> runningTxNums) {
        this.txNum = txNum;
        this.runningTxNums = runningTxNums;
    }

    @Override
    public byte[] toBytes() {
        return LogRecord.toBytes(operator, txNum, Optional.of(runningTxNums));
    }

    @Override
    public String toString() {
        return "<" + LogRecord.operatorToString(operator) + ", tx: " + txNum + ", runningTxNums: " + runningTxNums.toString() + ">";
    }

    // Does nothing, because a checkpoint record contains no undo information.
    @Override
    public void undo(Transaction tx) {}

    // Does nothing, because a checkpoint record contains no redo information.
    @Override
    public void redo(Transaction tx) {}

    public static long writeToLog(LogMgr logMgr, long txNum, ArrayList<Long> runningTxNums) {
        return LogRecord.writeToLog(logMgr, LogRecord.NQ_CHECKPOINT, txNum, Optional.of(runningTxNums));
    }
}
