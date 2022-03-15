package File;

import Error.EnumFileError;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

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
     * A constructor for creating log pages. Allows us to bulk load data into a page.
     * Whenever creating a page, need to allocate empty byte array first, then wrap
     * the array with a new page.
     * @param blob The byte array we want to store
     */
    public Page(byte[] blob) {
        this.buffer = ByteBuffer.wrap(blob);
        this.buffer.rewind();
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
     * Gets the short at the offset position from this page's buffer.
     * @param offset The position in the buffer where the short is located.
     * @return The short at the offset location
     */
    public short getShort(int offset) {
        return buffer.getShort(offset);
    }

    /**
     * Gets the {@link Byte} at the offset position from this page's buffer.
     * @param offset The position in the buffer where the byte is located.
     * @return The byte at the offset location.
     */
    public byte getByte(int offset) {
        return buffer.get(offset);
    }

    /**
     * Gets the short at the offset position from this page's buffer and then
     * converts it into a boolean.
     * @param offset The position in the buffer where the boolean (short) is located.
     * @return The boolean at the offset location.
     */
    public boolean getBoolean(int offset) {
        return getByte(offset) == 1;
    }

    /**
     * Gets the long at the offset position from this page's buffer.
     * @param offset The position in the buffer where the long is located.
     * @return The long at the offset location
     */
    public long getLong(int offset) {
        return buffer.getLong(offset);
    }

    /**
     * Gets the double at the offfset position from this page's buffer.
     * @param offset The position in the buffer where the double is located
     * @return The double at the offset location
     */
    public double getDouble(int offset) {
        return buffer.getDouble(offset);
    }

    /**
     * Since the DateTime is actually stored as a long (the epoch), this gets
     * the long at the offset position from this page's buffer. It then converts the
     * epoch back into a LocalDateTime object in the system's default timezone.
     * @param offset The position in the buffer where the long (epoch) is located.
     * @return The datetime object in the running system's default timezone.
     */
    public LocalDateTime getDateTime(int offset) {
        long epoch = getLong(offset);
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(epoch), ZoneId.systemDefault());
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
     * Gets the byte array at the offset position and converts into an ASCII
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
    public void setInt(int offset, int val) throws Exception {
        if (isTooBig(offset, val))
            throw new Exception(EnumFileError.BYTEBUFFER_TOO_FULL.toString());
        this.buffer.putInt(offset, val);
    }

    /**
     * Stores the provided byte array into the buffer at the specified offset position.
     * @param offset The position in the buffer we want to insert at.
     * @param blob The byte array we want to insert into the buffer.
     * @throws Exception Signifies data was too big to be inserted into the page.
     */
    public void setBytes(int offset, byte[] blob) throws Exception {
        if (isTooBig(offset, blob))
            throw new Exception(EnumFileError.BYTEBUFFER_TOO_FULL.toString());

        buffer.position(offset);
        buffer.putInt(blob.length);
        buffer.put(blob);
    }

    /**
     * Store a string in the buffer at the specified offset position.
     * @param offset The position in the buffer we want to insert at.
     * @param val The String we want to insert into the buffer.
     * @throws Exception Signifies String was too big to be inserted into the page.
     */
    public void setString(int offset, String val) throws Exception {
        byte[] bArr = val.getBytes(CHARSET);
        setBytes(offset, bArr);
    }

    /**
     * Store a boolean in the buffer at the specified offset position.
     * Boolean is actually stored as a short. True = 1, False = 0;
     * @param offset The position in the buffer we want to insert at.
     * @param bool The boolean we want to insert into the buffer.
     */
    public void setBoolean(int offset, boolean bool) {
        byte byteBool = bool ? (byte)1 : (byte)0;
        setByte(offset, byteBool);
    }

    /**
     * Actually converts the DateTime format into epoch seconds for more compact storage.
     * Can be retrieved and reconverted back into LocalDate object. Date is first
     * converted to UTC time and then stored as epoch seconds.
     * @param offset The position in the buffer we want to store this date at.
     * @param dateTime The DateTime we want to store.
     */
    public void setDateTime(int offset, LocalDateTime dateTime) {
        long epoch = dateTime.toEpochSecond(ZoneOffset.UTC);

        setLong(offset, epoch);
    }

    /**
     * Store a short in the buffer at the specified offset position.
     * @param offset The position in the buffer we want to insert at.
     * @param val short we want to insert into the buffer.
     */
    public void setShort(int offset, short val) {
        buffer.putShort(offset, val);
    }

    /**
     * Store a {@link Byte} in the buffer at the specified offset position.
     * @param offset The position in the buffer we want to insert at.
     * @param val The byte we want to insert into the buffer
     */
    public void setByte(int offset, byte val) {
        buffer.put(offset, val);
    }

    /**
     * Store a long in the buffer at the specified offset position.
     * @param offset The position in the buffer we want to insert at.
     * @param val The long we want to insert into the buffer.
     */
    public void setLong(int offset, long val) {
        buffer.putLong(offset, val);
    }

    /**
     * Store a double in the buffer at the specified offset position.
     * @param offset The position in the buffer we want to insert at.
     * @param val The double we want to insert into the buffer
     */
    public void setDouble(int offset, double val) {
        buffer.putDouble(offset, val);
    }

    /**
     * Checks to make sure that the information being inserted into the buffer
     * won't cause a BufferOverflowException. Information being inserted at the offset
     * needs to be checked that it won't go out of bounds.
     * @param blob The information we need to insert into the buffer.
     * @param offset The offset position we are trying to insert at.
     * @return True if the data is too big. False otherwise.
     */
    private boolean isTooBig(int offset, byte[] blob) {
        return offset + blob.length > buffer.capacity();
    }

    /**
     * Checks to make sure that the information being inserted into the buffer
     * won't cause a BufferOverflowException. Information being inserted at the offset
     * needs to be checked that it won't go out of bounds.
     * @param offset The offset position we are trying to insert at.
     * @param val Any Integer. Arg is just used for method overloading.
     * @return True if the data is too big. False otherwise.
     */
    private boolean isTooBig(int offset, int val) {
        return offset + Integer.BYTES > buffer.capacity();
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

    /**
     * Clears the contents of the page.
     */
    public void clear() {
        this.buffer.clear();
    }
}

