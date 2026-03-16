package Transaction.Recovery;

import Buffer.Buffer;
import Buffer.BufferMgr;
import File.BlockId;
import Log.LogMgr;
import lombok.extern.slf4j.Slf4j;
import Server.ConfigFetcher;
import Transaction.Transaction;
import Transaction.Recovery.RecordType.*;

import java.time.LocalDateTime;
import java.util.*;

/**
 * Unfortunately, the BufferMgr is implemented with a "steal" policy, aka, buffers can be flushed to disk at any time in
 * order to get pages to clients. It will flush a page to disk to pin a new block to it's buffer. This means that we
 * can't implement redo-only logging since we need to make sure the that COMMIT log record is written BEFORE the buffer
 * changes get flushed to disk for that strategy to work.
 *
 * However, we can implement the undo-redo strategy because we have already implemented write ahead logging (WAL)
 */

@Slf4j(topic = "RecoveryMgr")
public class RecoveryMgr {

    private LogMgr logMgr;

    private BufferMgr bufferMgr;

    private Transaction tx;

    private long txNum;

    public RecoveryMgr(Transaction tx, long txNum, LogMgr logMgr, BufferMgr bufferMgr) {
        this.txNum = txNum;
        this.logMgr = logMgr;
        this.bufferMgr = bufferMgr;
        this.tx = tx;

        StartRecord.writeToLog(logMgr, txNum);
    }

    public void commit() {
        bufferMgr.flushAll(txNum);
        long lsn = CommitRecord.writeToLog(logMgr, txNum);
        logMgr.flush(lsn);
    }

    public void rollback() {
        doRollback();
        bufferMgr.flushAll(txNum);
        long lsn = RollbackRecord.writeToLog(logMgr, txNum);
        logMgr.flush(lsn);
    }

    public void recover() {
        doRecover();
        bufferMgr.flushAll(txNum);
        manualCheckpoint();
    }

    private void manualCheckpoint() {
        long lsn = -1;
        if (ConfigFetcher.useNQCheckpointing())
            lsn = NQCheckpointRecord.writeToLog(logMgr, txNum, new ArrayList<>());
        else
            lsn = CheckpointRecord.writeToLog(logMgr, txNum);

        logMgr.flush(lsn);
    }

    private void doRollback() {
        Iterator<byte[]> recordBytesIter = logMgr.iterator();
        while (recordBytesIter.hasNext()) {
            byte[] recordBytes = recordBytesIter.next();
            LogRecord record = LogRecordFactory.convertToLogRecord(recordBytes);

            if (record.getTxNum() == txNum) {
                if (record.getOperator() == LogRecord.START)
                    return;

                record.undo(tx);
            }
        }
    }

    private void doRecover() {
        switch (ConfigFetcher.getRecoveryMgrStrategy()) {
            case RecoveryMgrStrategy.UNDO_REDO -> doRecoverUndoRedo();
            default -> doRecoverUndoOnly();
        }
    }

    private void doRecoverUndoRedo() {
        Deque<LogRecord> records = getLogsUntilCheckpoint();
        Set<Long> incompleteTxs = redoTxs(records);

        if (!incompleteTxs.isEmpty())
            undoIncompleteTxs(records, incompleteTxs);
    }

    private void undoIncompleteTxs(Deque<LogRecord> records, Set<Long> incompleteTxs) {
        // Need to invert the records stack so we go from newest log records backwards in time to oldest record
        Iterator<LogRecord> inverseRecordsStack = records.descendingIterator();

        while (inverseRecordsStack.hasNext()) {
            LogRecord record = inverseRecordsStack.next();

            if (incompleteTxs.contains(record.getTxNum())) {
                if (record.getOperator() == LogRecord.START)
                    incompleteTxs.remove(record.getTxNum());
                else
                    record.undo(tx);
            }

            if (incompleteTxs.isEmpty())
                break;
        }
    }

    private Set<Long> redoTxs(Deque<LogRecord> records) {
        Set<Long> incompleteTxs = new HashSet<>();
        Set<Long> committedTxs = new HashSet<>();

        // Going in the forwards order of the stack, we get the forwards direction of the log stream (oldest to newest)
        for (LogRecord record : records) {
            if (record.getOperator() == LogRecord.COMMIT) {
                committedTxs.add(record.getTxNum());
                incompleteTxs.remove(record.getTxNum());
            }
            else if (record.getOperator() == LogRecord.ROLLBACK) {
                incompleteTxs.remove(record.getTxNum());
            }
            else if (record.getOperator() == LogRecord.START) {
                incompleteTxs.add(record.getTxNum());
            }
        }

        // Need a second pass to redo only transactions that never completed
        for (LogRecord record : records) {
            if (record.isDataRecord() && committedTxs.contains(record.getTxNum()))
                record.redo(tx);
        }

        log.debug("NQ Checkpoint recovery. Incomplete Tx's to undo: {}", incompleteTxs);
        return incompleteTxs;
    }

    private Deque<LogRecord> getLogsUntilCheckpoint() {
        Deque<LogRecord> recordsStack = new ArrayDeque<>();
        Set<Long> startedTransactions = new HashSet<>();
        List<Long> nqRunningTransactions = new ArrayList<>();
        boolean foundNQCheckpoint = false;
        Iterator<byte[]> recordBytesIter = logMgr.iterator();

        while (recordBytesIter.hasNext()) {
            byte[] recordBytes = recordBytesIter.next();
            LogRecord record = LogRecordFactory.convertToLogRecord(recordBytes);

            if (record.getOperator() == LogRecord.START)
                startedTransactions.add(record.getTxNum());

            // If we exit the loop, but never hit a checkpoint, assume we hit the end of the log file
            if (record.getOperator() == LogRecord.CHECKPOINT)
                break;

            if (record.getOperator() == LogRecord.NQ_CHECKPOINT) {
                foundNQCheckpoint = true;
                NQCheckpointRecord nqRecord = (NQCheckpointRecord) record; // Need to do this to cast the LogRecord
                nqRunningTransactions = nqRecord.getRunningTxNums();
                continue;
            }

            recordsStack.push(record);

            // If we've seen all the running Tx's from the NQ checkpoint start records, then we know we've gone far
            // enough back in the logs. We can stop now. Also we need the foundNQCheckpoint flag because if the
            // nqRunningTransactions list is empty (a valid case), this will prematurely return since our
            // nqRunningTransactions list is empty until we populate it with a NQCheckpoint record
            if (foundNQCheckpoint && startedTransactions.containsAll(nqRunningTransactions))
                break;
        }

        if (recordsStack.isEmpty())
            log.debug("The record stack is empty! May be an issue, may be valid.");

        return recordsStack;
    }

    private void doRecoverUndoOnly() {
        Set<Long> finishedTransactions = new HashSet<>();
        Set<Long> startedTransactions = new HashSet<>();
        List<Long> nqRunningTransactions = new ArrayList<>();
        boolean foundNQCheckpoint = false;
        Iterator<byte[]> recordBytesIter = logMgr.iterator();

        while (recordBytesIter.hasNext()) {
            byte[] recordBytes = recordBytesIter.next();
            LogRecord record = LogRecordFactory.convertToLogRecord(recordBytes);

            if (record.getOperator() == LogRecord.START)
                startedTransactions.add(record.getTxNum());

            // If we hit a checkpoint, we can assume everything before the checkpoint is durable, so we can exit now
            if (record.getOperator() == LogRecord.CHECKPOINT)
                return;

            // If we see an NQ checkpoint, we need to ensure that we've gone back and seen the start of every
            // transaction that was running during the checkpoint. We will undo them if we didn't see a corresponding
            // termination for those transactions (commit/rollback)
            if (record.getOperator() == LogRecord.NQ_CHECKPOINT) {
                foundNQCheckpoint = true;
                NQCheckpointRecord nqRecord = (NQCheckpointRecord) record; // Need to do this to cast the LogRecord
                nqRunningTransactions = nqRecord.getRunningTxNums();
                continue;
            }

            if (record.getOperator() == LogRecord.COMMIT || record.getOperator() == LogRecord.ROLLBACK) {
                finishedTransactions.add(record.getTxNum());
            }
            else if (!finishedTransactions.contains(record.getTxNum())) {
                record.undo(tx);
            }

            // If we've seen all the running Tx's from the NQ checkpoint start records, then we know we've gone far
            // enough back in the logs. We can stop now. Also we need the foundNQCheckpoint flag because if the
            // nqRunningTransactions list is empty (a valid case), this will prematurely return since our
            // nqRunningTransactions list is empty until we populate it with a NQCheckpoint record
            if (foundNQCheckpoint && startedTransactions.containsAll(nqRunningTransactions))
                return;
        }
    }

    private boolean isSetUndoRedoStrategy() {
        return ConfigFetcher.getRecoveryMgrStrategy() == RecoveryMgrStrategy.UNDO_REDO;
    }

    public long setInt(Buffer buffer, int offset, int newValue) {
        int oldValue = buffer.getContents().getInt(offset);
        BlockId block = buffer.getBlock();

        if (isSetUndoRedoStrategy())
            return SetIntRecord.writeToLog(logMgr, txNum, block, offset, oldValue, newValue);

        return SetIntRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setString(Buffer buffer, int offset, String newValue) {
        String oldValue = buffer.getContents().getString(offset);
        BlockId block = buffer.getBlock();

        if (isSetUndoRedoStrategy())
            return SetStringRecord.writeToLog(logMgr, txNum, block, offset, oldValue, newValue);

        return SetStringRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setShort(Buffer buffer, int offset, short newValue) {
        short oldValue = buffer.getContents().getShort(offset);
        BlockId block = buffer.getBlock();

        if (isSetUndoRedoStrategy())
            return SetShortRecord.writeToLog(logMgr, txNum, block, offset, oldValue, newValue);

        return SetShortRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setByte(Buffer buffer, int offset, byte newValue) {
        byte oldValue = buffer.getContents().getByte(offset);
        BlockId block = buffer.getBlock();

        if (isSetUndoRedoStrategy())
            return SetByteRecord.writeToLog(logMgr, txNum, block, offset, oldValue, newValue);

        return SetByteRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setBoolean(Buffer buffer, int offset, boolean newValue) {
        boolean oldValue = buffer.getContents().getBoolean(offset);
        BlockId block = buffer.getBlock();

        if (isSetUndoRedoStrategy())
            return SetBooleanRecord.writeToLog(logMgr, txNum, block, offset, oldValue, newValue);

        return SetBooleanRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setLong(Buffer buffer, int offset, long newValue) {
        long oldValue = buffer.getContents().getLong(offset);
        BlockId block = buffer.getBlock();

        if (isSetUndoRedoStrategy())
            return SetLongRecord.writeToLog(logMgr, txNum, block, offset, oldValue, newValue);

        return SetLongRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setDouble(Buffer buffer, int offset, double newValue) {
        double oldValue = buffer.getContents().getDouble(offset);
        BlockId block = buffer.getBlock();

        if (isSetUndoRedoStrategy())
            return SetDoubleRecord.writeToLog(logMgr, txNum, block, offset, oldValue, newValue);

        return SetDoubleRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setDateTime(Buffer buffer, int offset, LocalDateTime newValue) {
        LocalDateTime oldValue = buffer.getContents().getDateTime(offset);
        BlockId block = buffer.getBlock();

        if (isSetUndoRedoStrategy())
            return SetDateTimeRecord.writeToLog(logMgr, txNum, block, offset, oldValue, newValue);

        return SetDateTimeRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }


}
