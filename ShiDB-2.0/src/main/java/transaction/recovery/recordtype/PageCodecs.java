package transaction.recovery.recordtype;

import file.Page;

import java.time.LocalDateTime;

public final class PageCodecs {
    public static final PageCodec<Integer> INT = new PageCodec<>() {
        public Integer read(Page page, int offset) {
            return page.getInt(offset);
        }

        public void write(Page page, int offset, Integer value) {
            page.setInt(offset, value);
        }

        public int byteSize(Integer value) {
            return Integer.BYTES;
        }
    };

    public static final PageCodec<String> STRING = new PageCodec<String>() {
        @Override
        public String read(Page page, int offset) {
            return page.getString(offset);
        }

        @Override
        public void write(Page page, int offset, String value) {
            page.setString(offset, value);
        }

        // IF I GET BUGS FROM OFFSET ISSUES, CHECK THIS. THE MAX BYTE LENGTH ISN'T THE SAME AS THE ACTUAL BYTE LENGTH
        @Override
        public int byteSize(String value) {
            return Page.calcMaxByteLength(value);
        }
    };

    public static final PageCodec<Byte> BYTE = new PageCodec<Byte>() {
        @Override
        public Byte read(Page page, int offset) {
            return page.getByte(offset);
        }

        @Override
        public void write(Page page, int offset, Byte value) {
            page.setByte(offset, value);
        }

        @Override
        public int byteSize(Byte value) {
            return Byte.BYTES;
        }
    };

    public static final PageCodec<Short> SHORT = new PageCodec<Short>() {
        @Override
        public Short read(Page page, int offset) {
            return page.getShort(offset);
        }

        @Override
        public void write(Page page, int offset, Short value) {
            page.setShort(offset, value);
        }

        @Override
        public int byteSize(Short value) {
            return Short.BYTES;
        }
    };

    public static final PageCodec<Boolean> BOOLEAN  = new PageCodec<Boolean>() {
        @Override
        public Boolean read(Page page, int offset) {
            return page.getBoolean(offset);
        }

        @Override
        public void write(Page page, int offset, Boolean value) {
            page.setBoolean(offset, value);
        }

        @Override
        public int byteSize(Boolean value) {
            return Byte.BYTES;
        }
    };

    public static final PageCodec<Long> LONG = new PageCodec<Long>() {
        @Override
        public Long read(Page page, int offset) {
            return page.getLong(offset);
        }

        @Override
        public void write(Page page, int offset, Long value) {
            page.setLong(offset, value);
        }

        @Override
        public int byteSize(Long value) {
            return Long.BYTES;
        }
    };

    public static final PageCodec<Double> DOUBLE = new PageCodec<Double>() {
        @Override
        public Double read(Page page, int offset) {
            return page.getDouble(offset);
        }

        @Override
        public void write(Page page, int offset, Double value) {
            page.setDouble(offset, value);
        }

        @Override
        public int byteSize(Double value) {
            return Double.BYTES;
        }
    };

    public static final PageCodec<LocalDateTime> DATE_TIME = new PageCodec<LocalDateTime>() {
        @Override
        public LocalDateTime read(Page page, int offset) {
            return page.getDateTime(offset);
        }

        @Override
        public void write(Page page, int offset, LocalDateTime value) {
            page.setDateTime(offset, value);
        }

        @Override
        public int byteSize(LocalDateTime value) {
            return Long.BYTES;
        }
    };
}
