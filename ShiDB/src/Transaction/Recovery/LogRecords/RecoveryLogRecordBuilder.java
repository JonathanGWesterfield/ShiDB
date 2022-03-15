package Transaction.Recovery.LogRecords;

import java.time.LocalDateTime;
import java.util.stream.Stream;

import File.BlockId;
import File.Page;

public class RecoveryLogRecordBuilder {

    private byte logOperation;
    private String filename;
    private int txNum;
    private int offset;
    private String value;
    private int blkNum;
    private Page recordPage;
    private int recordType;

    // Broader record classification to simplify Switch Statements
    private static final int checkpointRecord = 0;
    private static final int stateChangeRecord = 1;
    private static final int fullRecord = 2;

    // Strings for building the toString string
    private static String checkpointFormat = "<%s>"; // checkpoint record
    private static String stateChangeFormat = "<%s %d %d>"; // logOP, txNum, blk
    private static String fullRecordFormat = "<%s %d %d %d %s>"; // logOp, txNum, blk, offset, sVal

    public RecoveryLogRecordBuilder(byte logOperation) {
        this.logOperation = logOperation;

        switch (logOperation) {
        case LogRecord.CHECKPOINT:
            recordType = checkpointRecord;
            break;
        case LogRecord.START:
        case LogRecord.COMMIT:
        case LogRecord.ROLLBACK:
            recordType = stateChangeRecord;
            break;
        case LogRecord.SETINT:
        case LogRecord.SETSTRING:
        case LogRecord.SETBOOLEAN:
        case LogRecord.SETBYTE:
        case LogRecord.SETLONG:
        case LogRecord.SETDATETIME:
            recordType = fullRecord;
            break;
        }
    }

    public Page getRecordPage() {
        if (recordPage == null)
            throw new NullPointerException("The record page hasn't been created yet! Need to build() first!!!");
        return recordPage;
    }

    //
    public RecoveryLogRecordBuilder setLogOperation(byte logOperation) {
        this.logOperation = logOperation;
        return this;
    }

    public RecoveryLogRecordBuilder setTxNum(int txNum) {
        this.txNum = txNum;
        return this;
    }

    public RecoveryLogRecordBuilder setFilename(String filename) {
        this.filename = filename;
        return this;
    }

    public RecoveryLogRecordBuilder setBlockNum(BlockId blk) {
        this.blkNum = blk.getBlkNum();
        return this;
    }

    public RecoveryLogRecordBuilder setValueOffset(int valueOffset) {
        this.offset = valueOffset;
        return this;
    }

    public RecoveryLogRecordBuilder setValue(String value) {
        this.value = value;
        return this;
    }

    public RecoveryLogRecordBuilder setValue(int value) {
        this.value = Integer.toString(value);
        return this;
    }

    public RecoveryLogRecordBuilder setValue(long value) {
        this.value = Long.toString(value);
        return this;
    }

    public RecoveryLogRecordBuilder setValue(Boolean value) {
        this.value = Boolean.toString(value);
        return this;
    }

    public RecoveryLogRecordBuilder setValue(LocalDateTime dateTime) {
        this.value = dateTime.toString();
        return this;
    }

    public Page build() throws Exception {
        if (recordType == checkpointRecord && this.hasAllRequiredValues())
            return buildQCheckpointLogRecord();

        if (recordType == stateChangeRecord && this.hasAllRequiredValues())
            return buildStateChangeLogRecord();

        if (recordType == fullRecord && this.hasAllRequiredValues())
            return buildFullRecord();

        return null;
    }

    private Page buildQCheckpointLogRecord() {
        int transactionPosition = Byte.BYTES;
        int filenamePosition = transactionPosition + Integer.BYTES;
        int recordLength = filenamePosition + Page.maxLength(filename.length());

        byte[] record = new byte[recordLength];
        recordPage = new Page(record);
        recordPage.setByte(0, logOperation);

        return recordPage;
    }

    // TODO: IMPLEMENT THIS
    private Page buildStateChangeLogRecord() {
        return null;
    }

    private Page buildFullRecord() throws Exception {
        int transactionPosition = Byte.BYTES;
        int filenamePosition = transactionPosition + Integer.BYTES;
        int blockPosition = filenamePosition + Page.maxLength(filenamePosition);
        int offsetPosition = blockPosition + Integer.BYTES;
        int valuePosition = offsetPosition + Integer.BYTES;
        int recordLength = valuePosition + getValueLength();

        byte[] record = new byte[recordLength];
        recordPage = new Page(record);

        recordPage.setByte(0, logOperation);
        recordPage.setInt(transactionPosition, txNum);
        recordPage.setString(filenamePosition, filename);
        recordPage.setInt(blockPosition, blkNum);
        recordPage.setInt(offsetPosition, offset);

        switch (logOperation) {
        case LogRecord.SETINT:
            recordPage.setInt(valuePosition, Integer.parseInt(value));
            break;
        case LogRecord.SETSTRING:
            recordPage.setString(valuePosition, value);
            break;
        case LogRecord.SETLONG:
            recordPage.setLong(valuePosition, Long.parseLong(value));
            break;
        case LogRecord.SETDATETIME:
            recordPage.setDateTime(valuePosition, LocalDateTime.parse(value));
            break;
        case LogRecord.SETBOOLEAN:
            recordPage.setBoolean(valuePosition, Boolean.parseBoolean(value));
            break;
        case LogRecord.SETBYTE:
            recordPage.setByte(valuePosition, Byte.parseByte(value));
            break;
        }

        return recordPage;
    }

    private int getValueLength() {
        switch (logOperation) {
        case LogRecord.SETINT:
            return Integer.BYTES;
        case LogRecord.SETSTRING:
            return Page.maxLength(value.length());
        case LogRecord.SETLONG:
            return Long.BYTES;
        case LogRecord.SETDATETIME:
            return Long.BYTES;
        case LogRecord.SETBOOLEAN:
            return Byte.BYTES;
        case LogRecord.SETBYTE:
            return Byte.BYTES;
        default:
            return 0;
        }
    }

    private boolean hasAllRequiredValues() {
        switch (recordType) {
            case checkpointRecord:
                return filename != null && !filename.isEmpty();
            case stateChangeRecord:
                return Stream.of(filename, txNum, blkNum).noneMatch(x -> x == null);
            case fullRecord:
                return Stream.of(filename, txNum, blkNum, offset, value).noneMatch(x -> x == null);
        }
        return false;
    }

    @Override
    public String toString() {
        String logOp = "NULL_LOG_OP";
        String logFormat = "";
        switch (logOperation) {
        case LogRecord.CHECKPOINT:
            logOp = "CHECKPOINT";
            logFormat = checkpointFormat;
            break;
        case LogRecord.START:
            logOp = "START";
            logFormat = stateChangeFormat;
            break;
        case LogRecord.COMMIT:
            logOp = "COMMIT";
            logFormat = stateChangeFormat;
            break;
        case LogRecord.ROLLBACK:
            logOp = "ROLLBACK";
            logFormat = stateChangeFormat;
            break;
        case LogRecord.SETINT:
            logOp = "SETINT";
            logFormat = fullRecordFormat;
            break;
        case LogRecord.SETSTRING:
            logOp = "SETSTRING";
            logFormat = fullRecordFormat;
            break;
        }

        switch (recordType) {
        case checkpointRecord:
            return String.format(logFormat, logOp);
        case stateChangeRecord:
            return String.format(logFormat, logOp, txNum, blkNum);
        case fullRecord:
            return String.format(logFormat, logOp, txNum, blkNum, offset, value);
        }

        return logOp;
    }
}