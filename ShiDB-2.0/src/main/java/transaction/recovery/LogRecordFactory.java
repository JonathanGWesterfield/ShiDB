package transaction.recovery;

import file.Page;

public class LogRecordFactory {

    // I really hate the circular dependency of this factory method being in the LogRecord interface like the
    // book describes. This factory class scratches that itch.
    static LogRecord createLogRecord(byte[] bytes) {
        Page page = new Page(bytes);

        int logRecordType = page.getInt(0);
        return switch (logRecordType) {
            case LogRecord.CHECKPOINT -> new CheckpointRecord(page);
            case LogRecord.START -> new StartRecord(page);
            case LogRecord.COMMIT -> new CommitRecord(page);
            case LogRecord.ROLLBACK -> new RollbackRecord(page);
            case LogRecord.SET_STRING -> new SetStringRecord(page);
            case LogRecord.SET_INT -> new SetIntRecord();

            // The textbook returns null, but I'll be damned if I do that. NO NPE's!!!
            default -> throw new RuntimeException("Encountered an unsupported LogRecordType: " + logRecordType);
        };
    }
}
