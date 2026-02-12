package buffer;

/* This is an attempt at clean retry logic. I hate return null because:
 * 1. I hate null pointer exceptions
 * 2. I hate having to manually check for null every time
 *
 * I don't want to use Optional because while it solves point 1, it just dresses up point 2 with lipstick. I still
 * have to manually check for null, just using the Optional class as a wrapper
 *
 * I would like to use exceptions, but they really aren't meant for that and they are very computationally expensive
 * to generate the stack traces, so let's try creating a custom class to indicate hasSucceeded, or a need to retry
 *
 * This is very similar to the LeakyAbstractions Github library, but I didn't want to import another Jar for something
 * this small (unless I need to use this in lots of places in which case I'm might replace this)
 */

public record Attempt<T>(boolean hasSucceeded, T value) {

    public boolean hasFailed() {
        return !hasSucceeded;
    }

    public static <T> Attempt<T> succeeded(T value) {
        return new Attempt<>(true, value);
    }

    public static <T> Attempt<T> retry() {
        return new Attempt<>(false, null);
    }

    // Identical to retry, but the function semantics make this a bit more readable for cases where we don't
    // explicitly need to retry
    public static <T> Attempt<T> failed() {
        return new Attempt<>(false, null);
    }
}
