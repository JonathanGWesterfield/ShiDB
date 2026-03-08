package error;

public class LockAbortException extends RuntimeException {
    public LockAbortException(String errorMessage) {
        super(errorMessage);
    }

    public LockAbortException(Throwable err) {
        super(err);
    }

    public LockAbortException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }

}
