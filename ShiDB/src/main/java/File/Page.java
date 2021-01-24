package File;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {

    private ByteBuffer buffer;
    private static final Charset CHARSET = StandardCharsets.US_ASCII;
    private int blockSize;

    /**
     * A constructor for creating data buffers.
     * @param blockSize Size of a page of memory (or disk block size)
     */
    public Page(int blockSize) {
        // storing the blocksize isn't needed. The buffer has a set length we can use.
        this.buffer = ByteBuffer.allocateDirect(blockSize);
    }

    /**
     * A constructor for creating log pages
     * @param blob The byte array we want to store
     */
    public Page(byte[] blob) {
        this.buffer.wrap(blob);
    }

    /**
     * Gets the integer at the offset position from this page's buffer.
     * @param offset The position in the buffer where our integer is located.
     * @return The integer at our offset location.
     */
    public int getInt(int offset) {
        return buffer.getInt(offset);
    }

    /**
     * Gets the raw byte array from the buffer at the specified offset position.
     * @param offset The position in the array our byte array blob is located.
     * @return The byte array at the specified position.
     */
    public byte[] getBytes(int offset) {
        buffer.position(offset);
        int length = buffer.getInt();
        byte[] bArr = new byte[length];
        buffer.get(bArr);

        return bArr;
    }

    /**
     * Gets the byte array at the offset position and converts into into an ASCII
     * String.
     * @param offset The position in the array the String is located at.
     * @return The String at the specified offset position.
     */
    public String getString(int offset) {
        byte[] bArr = getBytes(offset);
        return new String(bArr, CHARSET);
    }

    /**
     * Sets the integer at the
     * @param offset
     * @param val
     */
    public void setInt(int offset, int val) {
        this.buffer.putInt(val, offset);
    }

    /**
     * Stores the provided byte array into the buffer at the specified offset position.
     * @param offset The position in the buffer we want to insert at.
     * @param blob The byte array we want to insert into the buffer.
     */
    public void setBytes(int offset, byte[] blob) {
        buffer.position(offset);
        buffer.putInt(blob.length);
        buffer.put(blob);
    }

    /**
     * Store a string in the buffer at the specified offset position.
     * @param offset The position in the buffer we want to insert at.
     * @param val The String we want to insert into the buffer.
     */
    public void setString(int offset, String val) {
        byte[] bArr = val.getBytes(CHARSET);
        setBytes(offset, bArr);
    }

    /**
     * Gets the maximum length needed to store a string of a certain size
     * @param strLen The length of the string we need space for
     * @return The maximum space needed to store the string of input
     */
    public static int maxLength(int strLen) {
        // maxL = (num chars * sizeof(char)) + sizeof(int)
        float bytesPerChar = CHARSET.newDecoder().maxCharsPerByte();
        return Integer.BYTES + (strLen * (int)bytesPerChar);
    }

    /**
     * Returns the bytebuffer that makes up this page with the read position set
     * to 0 (the beginning of the buffer).
     * @return The buffer that makes up the page.
     */
    protected ByteBuffer contents() {
        this.buffer.position(0);
        return this.buffer;
    }
}
