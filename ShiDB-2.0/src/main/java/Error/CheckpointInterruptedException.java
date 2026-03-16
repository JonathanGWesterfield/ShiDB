package Error;

public class CheckpointInterruptedException extends RuntimeException {
    public CheckpointInterruptedException(String errorMessage) {
        super(errorMessage);
    }

    public CheckpointInterruptedException(Throwable err) {
        super(err);
    }

    public CheckpointInterruptedException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
