package Error;

public class NegativeLogSequenceNumberException extends RuntimeException {
    public NegativeLogSequenceNumberException(String errorMessage) {
        super(errorMessage);
    }

    public NegativeLogSequenceNumberException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
