package Error;

public class BufferAbortException extends RuntimeException {
    public BufferAbortException(String errorMessage) {
        super(errorMessage);
    }

    public BufferAbortException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
