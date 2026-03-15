package record;

import buffer.Attempt;
import file.BlockId;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import transaction.ShouldLog;
import transaction.Transaction;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.function.BiFunction;

import static java.sql.Types.*;

@Slf4j(topic = "RecordMgr")
public class RecordPage {
    // The textbook implements these flags as integers because the barebones simpleDB only implemented writing ints
    // to pages and file blocks. However, I implemented setting boolean flags (implemented as a single byte) so we can
    // use a more intuitive boolean value. We don't need to worry about byte alignment because all of our pages are
    // implemented using Java ByteBuffers. Not as efficient, but not using hardware instructions so don't need to worry
    public static final Boolean EMPTY = false, IN_USE = true;

    private static final int DID_NOT_FIND_SLOT = Integer.MIN_VALUE;

    private Map<Boolean, String> inUseFlagStrMap = Map.of(
            IN_USE, "IN USE",
            EMPTY, "EMPTY"
    );

    private Transaction tx;

    @Getter
    private BlockId block;

    private Layout layout;

    public RecordPage(Transaction tx, BlockId block, Layout layout) {
        this.tx = tx;
        this.block = block;
        this.layout = layout;

        tx.pin(block);
    }

    public <T> T getValue(int recordSlot, String fieldName, BiFunction<BlockId, Integer, T> valueGetter) {
        int fieldPosition = getSlotOffset(recordSlot) + layout.getFieldOffset(fieldName);
        return valueGetter.apply(block, fieldPosition);
    }

    public int getInt(int recordSlot, String fieldName) {
        return getValue(recordSlot, fieldName, (blk, fieldPos) -> tx.getInt(block, fieldPos));
    }

    public String getString(int recordSlot, String fieldName) {
        return getValue(recordSlot, fieldName, (blk, fieldPos) -> tx.getString(block, fieldPos));
    }

    public byte getByte(int recordSlot, String fieldName) {
        return getValue(recordSlot, fieldName, (blk, fieldPos) -> tx.getByte(block, fieldPos));
    }

    public boolean getBoolean(int recordSlot, String fieldName) {
        return getValue(recordSlot, fieldName, (blk, fieldPos) -> tx.getBoolean(block, fieldPos));
    }

    public long getLong(int recordSlot, String fieldName) {
        return getValue(recordSlot, fieldName, (blk, fieldPos) -> tx.getLong(block, fieldPos));
    }

    public double getDouble(int recordSlot, String fieldName) {
        return getValue(recordSlot, fieldName, (blk, fieldPos) -> tx.getDouble(block, fieldPos));
    }

    public LocalDateTime getDateTime(int recordSlot, String fieldName) {
        return getValue(recordSlot, fieldName, (blk, fieldPos) -> tx.getDateTime(block, fieldPos));
    }

    private int calcFieldPosition(int recordSlot, String fieldName) {
        return getSlotOffset(recordSlot) + layout.getFieldOffset(fieldName);
    }

    public void setInt(int recordSlot, String fieldName, int value) {
        tx.setInt(block, calcFieldPosition(recordSlot, fieldName), value, ShouldLog.OK_TO_LOG);
    }

    public void setString(int recordSlot, String fieldName, String value) {
        tx.setString(block, calcFieldPosition(recordSlot, fieldName), value, ShouldLog.OK_TO_LOG);
    }

    public void setByte(int recordSlot, String fieldName, byte value) {
        tx.setByte(block, calcFieldPosition(recordSlot, fieldName), value, ShouldLog.OK_TO_LOG);
    }

    public void setBoolean(int recordSlot, String fieldName, boolean value) {
        tx.setBoolean(block, calcFieldPosition(recordSlot, fieldName), value, ShouldLog.OK_TO_LOG);
    }

    public void setLong(int recordSlot, String fieldName, long value) {
        tx.setLong(block, calcFieldPosition(recordSlot, fieldName), value, ShouldLog.OK_TO_LOG);
    }

    public void setDouble(int recordSlot, String fieldName, double value) {
        tx.setDouble(block, calcFieldPosition(recordSlot, fieldName), value, ShouldLog.OK_TO_LOG);
    }

    public void setDateTime(int recordSlot, String fieldName, LocalDateTime value) {
        tx.setDateTime(block, calcFieldPosition(recordSlot, fieldName), value, ShouldLog.OK_TO_LOG);
    }

    public void delete(int recordSlot) {
        log.debug("Deleting record (setting flag to empty) at record slot {}", recordSlot);
        setFlag(recordSlot, EMPTY);
    }

    public void format() {
        log.debug("Formatting record page...");
        int recordslot = 0;
        while (isValidSlot(recordslot)) {
            int recordSlotOffset = getSlotOffset(recordslot);
            tx.setBoolean(block, recordSlotOffset, EMPTY, ShouldLog.DO_NOT_LOG);

            Schema schema = layout.getSchema();
            for (String fieldName : schema.getFields()) {
                int fieldPosition = recordSlotOffset + layout.getFieldOffset(fieldName);
                int schemaFieldType = schema.getFieldType(fieldName);
                switch (schemaFieldType) {
                    case INTEGER   -> tx.setInt(block, fieldPosition, 0, ShouldLog.DO_NOT_LOG);
                    case BOOLEAN   -> tx.setBoolean(block, fieldPosition, false, ShouldLog.DO_NOT_LOG);
                    case BIGINT    -> tx.setLong(block, fieldPosition, 0L, ShouldLog.DO_NOT_LOG);
                    case DOUBLE    -> tx.setDouble(block, fieldPosition, 0.0, ShouldLog.DO_NOT_LOG);
                    case TIMESTAMP -> tx.setLong(block, fieldPosition, 0L, ShouldLog.DO_NOT_LOG);
                    case CHAR      -> tx.setByte(block, fieldPosition, (byte) 0, ShouldLog.DO_NOT_LOG);
                    case VARCHAR   -> tx.setString(block, fieldPosition, "", ShouldLog.DO_NOT_LOG);
                    default -> throw new RuntimeException(String.format(
                            "Field %s, offset: %s has invalid field type: %s",
                            fieldName, fieldPosition, schemaFieldType));
                }
                log.debug("Formatted field={}, type={}, position={}", fieldName, schemaFieldType, fieldPosition);
            }
            recordslot++;
        }
    }

    public Attempt<Integer> nextSlotAfter(int recordSlot) {
        Attempt<Integer> slotSearch = searchForSlotAfter(recordSlot, IN_USE);
        if (slotSearch.hasSucceeded())
            return Attempt.succeeded(slotSearch.value());

        return Attempt.failed();
    }

    // Will attempt to insert a record after the given record slot. Since this could fail, and it will continue iterating
    // and searching for an empty block, will return the slot number that it eventually inserted into
    public Attempt<Integer> insertAfter(int recordSlot) {
        Attempt<Integer> newSlotSearchAttempt = searchForSlotAfter(recordSlot, EMPTY);
        if (newSlotSearchAttempt.hasSucceeded()) {
            int foundSlotNum = newSlotSearchAttempt.value();
            setFlag(foundSlotNum, IN_USE);
            return Attempt.succeeded(foundSlotNum);
        }

        return Attempt.failed();
    }

    private void setFlag(int recordSlot, boolean flag) {
        tx.setBoolean(block, getSlotOffset(recordSlot), flag, ShouldLog.OK_TO_LOG);
    }

    private Attempt<Integer> searchForSlotAfter(int recordSlot, boolean flag) {
        recordSlot++;

        log.debug("Searching for a slot after slot {} that is {} -> {}", recordSlot - 1, inUseFlagStrMap.get(flag), flag);

        int counter = 0;
        while (isValidSlot(recordSlot)) {
            int slotOffset = getSlotOffset(recordSlot);

            boolean valueAtSlot = tx.getBoolean(block, slotOffset);
            boolean foundMatchingSlot = valueAtSlot == flag;

            log.debug("Iteration {}: slotOffset={}, slotValue={}, foundMatching?={}",
                    counter, slotOffset, valueAtSlot, foundMatchingSlot);
            if (foundMatchingSlot)
                return Attempt.succeeded(recordSlot);

            recordSlot++;
            counter++;
        }

        return Attempt.failed();
    }

    public boolean isValidSlot(int recordSlot) {
        int slotOffset = getSlotOffset(recordSlot + 1);
        return slotOffset <= tx.blockSize();
    }

    public int getSlotOffset(int recordSlot) {
        return recordSlot * layout.getSlotSize();
    }
}
