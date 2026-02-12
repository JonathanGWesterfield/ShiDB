package error;

public class BufferAbortException extends RuntimeException {
    public BufferAbortException(String errorMessage) {
        super(errorMessage);
    }

    public BufferAbortException(Throwable err) {
        super(err);
    }

    public BufferAbortException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }

}
