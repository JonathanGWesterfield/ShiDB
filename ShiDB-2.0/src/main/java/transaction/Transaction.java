package transaction;

import buffer.Buffer;
import buffer.BufferMgr;
import file.BlockId;
import file.FileMgr;
import file.Page;
import log.LogMgr;
import lombok.extern.slf4j.Slf4j;
import transaction.concurrency.BufferList;
import transaction.concurrency.ConcurrencyMgr;
import transaction.concurrency.TransactionRegistrySingleton2;
import transaction.recovery.RecoveryMgr;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

@Slf4j
public class Transaction {

    private static AtomicLong nextTxNum = new AtomicLong(0L);

    private static final int END_OF_FILE = -1;

    private RecoveryMgr recoveryMgr;

    private ConcurrencyMgr concurrencyMgr;

    private BufferMgr bufferMgr;

    private FileMgr fileMgr;

    private long txNum;

    private BufferList ownedBuffers;

    public Transaction(FileMgr fileMgr, LogMgr logMgr, BufferMgr bufferMgr) {
        this.fileMgr = fileMgr;
        this.bufferMgr = bufferMgr;
        this.txNum = nextTxNum.incrementAndGet();

        log.debug("New Transaction #: {}", nextTxNum);

        // This needs to get fetched first to potentially stop any new transactions from starting if we need to set
        // a quiescent checkpoint
        TransactionRegistrySingleton2.getInstance().registerTx(txNum);

        recoveryMgr = new RecoveryMgr(this, txNum, logMgr, bufferMgr);
        concurrencyMgr = new ConcurrencyMgr();
        ownedBuffers = new BufferList(bufferMgr);
    }

    public void commit() {
        recoveryMgr.commit();
        concurrencyMgr.releaseAllLocks();
        ownedBuffers.unpinAll();
        TransactionRegistrySingleton2.getInstance().deRegisterTx(txNum);
        log.debug("Transaction: {} COMMITTED", txNum);
    }

    public void rollback() {
        recoveryMgr.rollback();
        concurrencyMgr.releaseAllLocks();
        ownedBuffers.unpinAll();
        TransactionRegistrySingleton2.getInstance().deRegisterTx(txNum);
        log.debug("Transaction: {} ROLLED BACK", txNum);
    }

    public void recover() {
        bufferMgr.flushAll(txNum);
        recoveryMgr.recover();
    }

    public void pin(BlockId block) {
        ownedBuffers.pin(block);
    }

    public void unPin(BlockId block) {
        ownedBuffers.unpin(block);
    }

    public short getShort(BlockId block, int offset) {
        return getValue(block, offset, Page::getShort);
    }

    public Boolean getBoolean(BlockId block, int offset) {
        return getValue(block, offset, Page::getBoolean);
    }

    public int getInt(BlockId block, int offset) {
        return getValue(block, offset, Page::getInt);
    }

    public String getString(BlockId block, int offset) {
        return getValue(block, offset, Page::getString);
    }

    public byte getByte(BlockId block, int offset) {
        return getValue(block, offset, Page::getByte);
    }

    public long getLong(BlockId block, int offset) {
        return getValue(block, offset, Page::getLong);
    }

    public double getDouble(BlockId block, int offset) {
        return getValue(block, offset, Page::getDouble);
    }

    public LocalDateTime getDateTime(BlockId block, int offset) {
        return getValue(block, offset, Page::getDateTime);
    }

    public void setShort(BlockId block, int offset, short newValue, ShouldLog shouldLog) {
        setValue(block, offset, shouldLog,
                (buffer, offsetPos) -> recoveryMgr.setShort(buffer, offsetPos, newValue),
                (page, offsetPos) -> page.setShort(offsetPos, newValue));
    }

    public void setBoolean(BlockId block, int offset, boolean newValue, ShouldLog shouldLog) {
        setValue(block, offset, shouldLog,
                (buffer, offsetPos) -> recoveryMgr.setBoolean(buffer, offsetPos, newValue),
                (page, offsetPos) -> page.setBoolean(offsetPos, newValue));
    }

    public void setInt(BlockId block, int offset, int newValue, ShouldLog shouldLog) {
        setValue(block, offset, shouldLog,
                (buffer, offsetPos) -> recoveryMgr.setInt(buffer, offsetPos, newValue),
                (page, offsetPos) -> page.setInt(offsetPos, newValue));
    }

    public void setString(BlockId block, int offset, String newValue, ShouldLog shouldLog) {
        setValue(block, offset, shouldLog,
                (buffer, offsetPos) -> recoveryMgr.setString(buffer, offsetPos, newValue),
                (page, offsetPos) -> page.setString(offsetPos, newValue));
    }

    public void setByte(BlockId block, int offset, byte newValue, ShouldLog shouldLog) {
        setValue(block, offset, shouldLog,
                (buffer, offsetPos) -> recoveryMgr.setByte(buffer, offsetPos, newValue),
                (page, offsetPos) -> page.setByte(offsetPos, newValue));
    }

    public void setLong(BlockId block, int offset, long newValue, ShouldLog shouldLog) {
        setValue(block, offset, shouldLog,
                (buffer, offsetPos) -> recoveryMgr.setLong(buffer, offsetPos, newValue),
                (page, offsetPos) -> page.setLong(offsetPos, newValue));
    }

    public void setDouble(BlockId block, int offset, double newValue, ShouldLog shouldLog) {
        setValue(block, offset, shouldLog,
                (buffer, offsetPos) -> recoveryMgr.setDouble(buffer, offsetPos, newValue),
                (page, offsetPos) -> page.setDouble(offsetPos, newValue));
    }

    public void setDateTime(BlockId block, int offset, LocalDateTime newValue, ShouldLog shouldLog) {
        setValue(block, offset, shouldLog,
                (buffer, offsetPos) -> recoveryMgr.setDateTime(buffer, offsetPos, newValue),
                (page, offsetPos) -> page.setDateTime(offsetPos, newValue));
    }

    public int size(String filename) {
        // We do this to ensure that no other transaction can append to the end of the file and invalidate the file
        // block length calculation before the user can use it.
        // For reading, we use a shared lock since multiple transactions should be able to read a block with no issues
        BlockId dummyBlock = new BlockId(filename, END_OF_FILE);
        concurrencyMgr.setSharedLock(dummyBlock);

        return fileMgr.numBlocksInFile(filename);
    }

    public BlockId append(String filename) {
        // We set this to ensure that no other transaction can append the end of the file and possibly invalidate
        // the size that the calling client expected. We use an exclusive lock because we need to make sure that no
        // other transaction can write and cause serializability issues
        BlockId dummyBlock = new BlockId(filename, END_OF_FILE);
        concurrencyMgr.setExclusiveLock(dummyBlock);

        return fileMgr.append(filename);
    }

    public int blockSize() {
        return fileMgr.getBlocksize();
    }

    public int getNumAvailableBuffers() {
        return bufferMgr.getNumAvailableBuffers();
    }

    private <T> T getValue(BlockId block, int offset, BiFunction<Page, Integer, T> pageReader) {
        concurrencyMgr.setSharedLock(block);
        Page page = ownedBuffers.getBuffer(block).getContents();
        return pageReader.apply(page, offset);
    }

    private void setValue(BlockId block, int offset, ShouldLog shouldLog,
                          BiFunction<Buffer, Integer, Long> recoverMgrLogger, BiConsumer<Page, Integer> pageWriter) {
        concurrencyMgr.setExclusiveLock(block);
        Buffer buffer = ownedBuffers.getBuffer(block);

        long lsn = -1;
        if (shouldLog == ShouldLog.OK_TO_LOG)
            lsn = recoverMgrLogger.apply(buffer, offset);

        Page page = buffer.getContents();
        pageWriter.accept(page, offset);
        buffer.setModified(txNum, lsn);
    }
}
