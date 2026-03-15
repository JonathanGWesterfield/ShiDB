package file;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import server.ConfigFetcher;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * A page holds a single block. This lets the block be read from the disk and modified in memory,
 * saving us from expensive I/O operations to read/write from the disk
 */
@Slf4j(topic = "FileMgr")
public class Page {
    private ByteBuffer byteBuffer;

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
        if (offset >= 0 && offset < byteBuffer.limit())
            return;

        if (offset < 0)
            throw new IllegalArgumentException(
                "Provided offset: " + offset + " - Cannot have a negative offset to the byte buffer!");

        if (offset >= byteBuffer.limit())
            throw new IllegalArgumentException("Provided offset: " + offset +
                    " is greater or equal to the bytebuffer limit: " + byteBuffer.limit());
    }

    public short getShort(int offset) {
        return byteBuffer.getShort(offset);
    }

    public byte getByte(int offset) {
        return byteBuffer.get(offset);
    }

    public boolean getBoolean(int offset) {
        return getByte(offset) == 1;
    }

    public long getLong(int offset) {
        return byteBuffer.getLong(offset);
    }

    public double getDouble(int offset) {
        return byteBuffer.getDouble(offset);
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
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(epoch), ZoneOffset.UTC);


    }

    public int getInt(int offset) {
        validateOffset(offset);

        return byteBuffer.getInt(offset);
    }

    // This check likely isn't needed as long as the buffer manager is implemented correctly
    public void validateValueWillFit(int offset, int byteLength) {
        if (byteBuffer.position(offset).remaining() >= byteLength)
            return;

        String errMsg = String.format(
                "Attempting to insert at offset %d failed because the object" +
                        " of %s bytes is too large for the %s byte block size limit!",
                offset, byteLength, byteBuffer.capacity());
        throw new RuntimeException(errMsg);
    }

    public void setInt(int offset, int val) {
        validateOffset(offset);
        validateValueWillFit(offset, Integer.BYTES);

        byteBuffer.putInt(offset, val);
    }

    public byte[] getBytes(int offset) {
        validateOffset(offset);

        byteBuffer.position(offset);
        int length = byteBuffer.getInt();

        byte[] byteArr = new byte[length];
        byteBuffer.get(byteArr);

        return byteArr;
    }

    public String getString(int offset, Charset charset) {
        byte[] byteStr = getBytes(offset);
        return new String(byteStr, charset);
    }

    public String getString(int offset) {
        return getString(offset, ConfigFetcher.getStringCharset());
    }

    public int getExactStringByteLength(int offset) {
        validateOffset(offset);

        byteBuffer.position(offset);
        int length = byteBuffer.getInt(offset);

        return length;
    }

    public void setBytes(int offset, byte[] val) {
        validateOffset(offset);
        validateValueWillFit(offset, val.length);

        byteBuffer.position(offset);
        byteBuffer.putInt(val.length);
        byteBuffer.put(val);
    }

    public void setBoolean(int offset, boolean bool) {
        // No need to validate offset and size since setByte() does that for us
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
        // No need to validate offset and size since setLong() does that for us
        long epoch = dateTime.toEpochSecond(ZoneOffset.UTC);

        setLong(offset, epoch);
    }

    public void setShort(int offset, short val) {
        validateOffset(offset);
        validateValueWillFit(offset, Short.BYTES);

        byteBuffer.putShort(offset, val);
    }

    public void setByte(int offset, byte val) {
        validateOffset(offset);
        validateValueWillFit(offset, Byte.BYTES);

        byteBuffer.put(offset, val);
    }

    public void setLong(int offset, long val) {
        validateOffset(offset);
        validateValueWillFit(offset, Long.BYTES);

        byteBuffer.putLong(offset, val);
    }

    public void setDouble(int offset, double val) {
        validateOffset(offset);
        validateValueWillFit(offset, Double.BYTES);

        byteBuffer.putDouble(offset, val);
    }

    public void setString(int offset, String value, Charset charset) {
        // No need to validate the offset since lower level functions will validate it

        byte[] byteStr = value.getBytes(charset);
        setBytes(offset, byteStr);
    }

    public void setString(int offset, String value) {
        setString(offset, value, ConfigFetcher.getStringCharset());
    }

    /**
     * Calculates the maximum number of bytes needed for given string. This is convenient when using a simple
     * charset like ASCII, but is absolutely critical for more complex charsets like UTF-16 where between 2 - 4
     * bytes can be used per character. Also accounts for the block size used at the beginning of a block
     * @param str The string to evaluate
     * @return The maximum number of bytes needed to properly store a string based on the charset used
     */
    public static int calcMaxByteLength(@NonNull String str) {
        return calcMaxByteLength(str.length());
    }

    public static int calcMaxByteLength(int stringLength) {
        return calcMaxByteLength(stringLength, ConfigFetcher.getStringCharset());
    }

    public static int calcMaxByteLength(int stringLength, Charset charset) {
        float bytesPerChar = charset.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (stringLength * (int) bytesPerChar);
    }

    public ByteBuffer getContents() {
        byteBuffer.position(0);
        return byteBuffer;
    }

    public void clear() {
        log.debug("Clearing the byte buffer by zeroing it out");
        log.debug("Bytebuffer before clearing: {}", ConfigFetcher.getStringCharset().decode(byteBuffer));
        byte[] zeros = new byte[byteBuffer.capacity()];
        byteBuffer.position(0);
        byteBuffer.put(zeros);
        byteBuffer.position(0);
        log.debug("Bytebuffer after clearing: {}", ConfigFetcher.getStringCharset().decode(byteBuffer));
    }
}
