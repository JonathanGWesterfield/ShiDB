package file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private ByteBuffer byteBuffer;

    // For the sake of simplicity, we will start with ASCII. No need to kill ourselves with UTF-8 yet
    public static final Charset CHARSET = StandardCharsets.US_ASCII;

    /**
     * Constructor used to create bytebuffers from scratch. Since I/O buffers are a valuable resource,
     * The buffer manager should really be the only thing calling this
     * @param blockSize Size of the blocks needed to allocate for this page
     */
    public Page(int blockSize) {
        byteBuffer = ByteBuffer.allocateDirect(blockSize);
    }

    /**
     * Constructor used for creating log pages
     * @param byteArr The byte array to wrap for the Page to wrap
     */
    public Page(byte[] byteArr) {
        byteBuffer = ByteBuffer.wrap(byteArr);
    }

    private void validateOffset(int offset) {
        if (offset >= 0)
            return;

        throw new IllegalArgumentException(
                "Provided offset: " + offset + " - Cannot have a negative offset to the byte buffer!");
    }

    public int getInt(int offset) {
        validateOffset(offset);

        return byteBuffer.getInt(offset);
    }

    public void setInt(int offset, int val) {
        validateOffset(offset);

        byteBuffer.putInt(offset, val);
    }

    public byte[] getBytes(int offset) {
        validateOffset(offset);

        byteBuffer.position(offset);
        int length = byteBuffer.getInt(offset);

        byte[] byteArr = new byte[length];
        byteBuffer.get(byteArr);

        return byteArr;
    }

    public void setBytes(int offset, byte[] val) {
        validateOffset(offset);

        byteBuffer.position(offset);
        byteBuffer.putInt(val.length);
        byteBuffer.put(val);
    }

    public String getString(int offset) {
        // No need to validate the offset since lower level functions will validate it
        byte[] byteStr = getBytes(offset);
        return new String(byteStr, CHARSET);
    }

    public void setString(int offset, String val) {
        // No need to validate the offset since lower level functions will validate it

        byte[] byteStr = val.getBytes(CHARSET);
        setBytes(offset, byteStr);
    }

    /**
     * Calculates the maximum number of bytes needed for given string. This is convenient when using a simple
     * charset like ASCII, but is absolutely critical for more complex charsets like UTF-16 where between 2 - 4
     * bytes can be used per character. Also accounts for the block size used at the beginning of a block
     * @param strLen Length of the string being evaluated
     * @return The maximum number of bytes needed to properly store a string based on the charset used
     */
    public static int calcMaxByteLength(int strLen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strLen * (int)bytesPerChar);
    }

    // Package method needed by the FileMgr class
    protected ByteBuffer getContents() {
        byteBuffer.position(0);
        return byteBuffer;
    }
}
