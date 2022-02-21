package Error;

public class BufferException extends RuntimeException {

    public BufferException(String errorMessage) {
        super(errorMessage);
    }

    public BufferException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
