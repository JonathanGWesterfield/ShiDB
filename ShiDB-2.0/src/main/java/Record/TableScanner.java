package Record;

import Buffer.Attempt;
import File.BlockId;
import lombok.extern.slf4j.Slf4j;
import Transaction.Transaction;

import java.time.LocalDateTime;
import java.util.NoSuchElementException;

@Slf4j(topic = "RecordMgr")
public class TableScanner {
    private static final int NULL_SLOT = -1;

    private Transaction tx;
    private Layout layout;
    private RecordPage recordPage;
    private String filename;
    private int currentSlot;
    private RecordID cachedNextRecordID;

    public TableScanner(Transaction tx, String tableName, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        this.filename = tableName + ".tbl";
        if (tx.fileSize(filename) == 0)
            moveToNewBlock();
        else
            moveToBlock(0);
    }

    // Methods that actually implement the table scan
    public void close() {
        if (recordPage != null)
            tx.unPin(recordPage.getBlock());
    }

    public void beforeFirst() {
        moveToBlock(0);
        cachedNextRecordID = null;
    }

    private Attempt<RecordID> findNextRid(int startBlock, int startSlot) {
        int blockNum = startBlock;
        int slot = startSlot;

        while (blockNum < tx.fileSize(filename)) {
            Attempt<Integer> attempt;

            if (blockNum == startBlock) {
                // reuse the already-pinned recordPage — don't create a new one
                attempt = recordPage.nextInUseSlotAfter(slot);
            } else {
                BlockId block = new BlockId(filename, blockNum);
                RecordPage page = new RecordPage(tx, block, layout);
                attempt = page.nextInUseSlotAfter(slot);
                tx.unPin(block);
            }

            if (attempt.hasSucceeded())
                return Attempt.succeeded(new RecordID(blockNum, attempt.value()));

            blockNum++;
            slot = NULL_SLOT;
        }

        return Attempt.failed();
    }

    public boolean hasNext() {
        if (cachedNextRecordID != null) return true;
        Attempt<RecordID> attempt = findNextRid(recordPage.getBlock().blockNum(), currentSlot);
        if (attempt.hasSucceeded()) {
            cachedNextRecordID = attempt.value();
            return true;
        }
        return false;
    }

    public TableScanner next() {
        if (cachedNextRecordID == null) {
            Attempt<RecordID> nextRIDSearchAttempt = findNextRid(recordPage.getBlock().blockNum(), currentSlot);
            if (nextRIDSearchAttempt.hasFailed())
                throw new NoSuchElementException("No more records in table " + filename +
                        "! Call the hasNext() function next time!");

            cachedNextRecordID = nextRIDSearchAttempt.value();
        }

        moveToRecordID(cachedNextRecordID);
        cachedNextRecordID = null;
        return this;
    }

//    @Deprecated
//    // I'm really not a fan of the next() function returning a boolean instead of a value like an iterator
//    public boolean next() {
//        Attempt<Integer> nextSlotAttempt = recordPage.nextInUseSlotAfter(currentSlot);
//        if (nextSlotAttempt.hasSucceeded()) {
//            currentSlot = nextSlotAttempt.value();
//            return true;
//        }
//
//        while(currentSlot == NULL_SLOT || nextSlotAttempt.hasFailed()) {
//            if (atLastBlock())
//                return false;
//
//            moveToBlock(recordPage.getBlock().blockNum() + 1);
//            nextSlotAttempt = recordPage.nextInUseSlotAfter(currentSlot);
//
//            if (nextSlotAttempt.hasSucceeded())
//                currentSlot = nextSlotAttempt.value();
//        }
//
//        return true;
//    }

    public void insert() {
        Attempt<Integer> insertAttempt = recordPage.insertInEmptySlotAfter(currentSlot);
        if (insertAttempt.hasSucceeded()) {
            currentSlot = insertAttempt.value();
            return;
        }
        else {
            while(insertAttempt.hasFailed()) {
                if (atLastBlock())
                    moveToNewBlock();
                else
                    moveToBlock(recordPage.getBlock().blockNum() + 1);

                insertAttempt = recordPage.insertInEmptySlotAfter(currentSlot);
            }
        }

        if (insertAttempt.hasFailed())
            throw new RuntimeException("Even after moving to a new/free block, failed to insert!");

        currentSlot = insertAttempt.value();
        cachedNextRecordID = null;
    }

    public int getInt(String fieldName) {
        return recordPage.getInt(currentSlot, fieldName);
    }

    public String getString(String fieldName) {
        return recordPage.getString(currentSlot, fieldName);
    }

    public byte getByte(String fieldName) {
        return recordPage.getByte(currentSlot, fieldName);
    }

    public boolean getBoolean(String fieldName) {
        return recordPage.getBoolean(currentSlot, fieldName);
    }

    public long getLong(String fieldName) {
        return recordPage.getLong(currentSlot, fieldName);
    }

    public double getDouble(String fieldName) {
        return recordPage.getDouble(currentSlot, fieldName);
    }

    public LocalDateTime getDateTime(String fieldName) {
        return recordPage.getDateTime(currentSlot, fieldName);
    }

    public void setInt(String fieldName, int value) {
        recordPage.setInt(currentSlot, fieldName, value);
    }

    public void setString(String fieldName, String value) {
        recordPage.setString(currentSlot, fieldName, value);
    }

    public void setByte(String fieldName, byte value) {
        recordPage.setByte(currentSlot, fieldName, value);
    }

    public void setBoolean(String fieldName, boolean value) {
        recordPage.setBoolean(currentSlot, fieldName, value);
    }

    public void setLong(String fieldName, long value) {
        recordPage.setLong(currentSlot, fieldName, value);
    }

    public void setDouble(String fieldName, double value) {
        recordPage.setDouble(currentSlot, fieldName, value);
    }

    public void setDateTime(String fieldName, LocalDateTime value) {
        recordPage.setDateTime(currentSlot, fieldName, value);
    }

    public void delete() {
        recordPage.delete(currentSlot);
        cachedNextRecordID = null;
    }

    public void moveToRecordID(RecordID recordID) {
        close();
        BlockId block = new BlockId(filename, recordID.blockNumber());
        recordPage = new RecordPage(tx, block, layout);
        currentSlot = recordID.slotNumber();
        cachedNextRecordID = null;
    }

    public RecordID getRecordId() {
        return new RecordID(recordPage.getBlock().blockNum(), currentSlot);
    }

    private void moveToBlock(int blockNum) {
        close();
        BlockId block = new BlockId(filename, blockNum);
        recordPage = new RecordPage(tx, block, layout);
        currentSlot = NULL_SLOT;
    }

    private void moveToNewBlock() {
        close();
        BlockId block = tx.append(filename);
        recordPage = new RecordPage(tx, block, layout);
        recordPage.format();
        currentSlot = NULL_SLOT;
    }

    private boolean atLastBlock() {
        return recordPage.getBlock().blockNum() == (tx.fileSize(filename) - 1);
    }
}
