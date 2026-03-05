package transaction.recovery;

import buffer.Buffer;
import buffer.BufferMgr;
import file.BlockId;
import log.LogMgr;
import lombok.extern.slf4j.Slf4j;
import transaction.Transaction;
import transaction.recovery.recordtype.*;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
        long lsn = CheckpointRecord.writeToLog(logMgr, txNum);
        logMgr.flush(lsn);
    }

    private void doRollback() {
        Iterator<byte[]> recordBytesIter = logMgr.iterator();
        while (recordBytesIter.hasNext()) {
            byte[] recordBytes = recordBytesIter.next();
            LogRecord record = LogRecordFactory.createLogRecord(recordBytes);

            if (record.getTxNum() == txNum) {
                if (record.getOperator() == LogRecord.START)
                    return;

                record.undo(tx);
            }
        }
    }

    private void doRecover() {
        Set<Long> finishedTransactions = new HashSet<>();
        Iterator<byte[]> recordBytesIter = logMgr.iterator();

        while (recordBytesIter.hasNext()) {
            byte[] recordBytes = recordBytesIter.next();
            LogRecord record = LogRecordFactory.createLogRecord(recordBytes);

            if (record.getOperator() == LogRecord.CHECKPOINT)
                return;

            if (record.getOperator() == LogRecord.COMMIT || record.getOperator() == LogRecord.ROLLBACK) {
                finishedTransactions.add(record.getTxNum());
            }
            else if (!finishedTransactions.contains(record.getTxNum())) {
                record.undo(tx);
            }
        }
    }

    // newValue is intentionally unused in case we need to implement redo or undo-redo recovery (which I likely will)
    // This pattern applies to all the other "set" functions
    public long setInt(Buffer buffer, int offset, int newValue) {
        int oldValue = buffer.getContents().getInt(offset);
        BlockId block = buffer.getBlock();
        return SetIntRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setString(Buffer buffer, int offset, String newValue) {
        String oldValue = buffer.getContents().getString(offset);
        BlockId block = buffer.getBlock();
        return SetStringRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setShort(Buffer buffer, int offset, short newValue) {
        short oldValue = buffer.getContents().getShort(offset);
        BlockId block = buffer.getBlock();
        return SetShortRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setByte(Buffer buffer, int offset, byte newValue) {
        byte oldValue = buffer.getContents().getByte(offset);
        BlockId block = buffer.getBlock();
        return SetByteRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setBoolean(Buffer buffer, int offset, boolean newValue) {
        boolean oldValue = buffer.getContents().getBoolean(offset);
        BlockId block = buffer.getBlock();
        return SetBooleanRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setLong(Buffer buffer, int offset, long newValue) {
        long oldValue = buffer.getContents().getLong(offset);
        BlockId block = buffer.getBlock();
        return SetLongRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setDouble(Buffer buffer, int offset, double newValue) {
        double oldValue = buffer.getContents().getDouble(offset);
        BlockId block = buffer.getBlock();
        return SetDoubleRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }

    public long setDateTime(Buffer buffer, int offset, LocalDateTime newValue) {
        LocalDateTime oldValue = buffer.getContents().getDateTime(offset);
        BlockId block = buffer.getBlock();
        return SetDateTimeRecord.writeToLog(logMgr, txNum, block, offset, oldValue);
    }


}
