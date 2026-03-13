package file;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class PageCharsetTest {

    static Stream<Charset> supportedCharsets() {
        return Stream.of(
                StandardCharsets.US_ASCII,
                StandardCharsets.UTF_8,
                StandardCharsets.UTF_16,
                StandardCharsets.UTF_32
        );
    }

    @ParameterizedTest(name = "getString round-trip [{0}]")
    @MethodSource("supportedCharsets")
    @DisplayName("getString returns exact string written by setString for each charset")
    void testStringRoundTrip(Charset charset) {
        Page page = new Page(512);
        String original = "ShiDB charset test";
        int offset = 0;

        page.setString(offset, original, charset);
        String retrieved = page.getString(offset, charset);

        assertEquals(original, retrieved);
    }

    @ParameterizedTest(name = "multi-byte characters [{0}]")
    @MethodSource("supportedCharsets")
    @DisplayName("Handles multi-byte characters correctly per charset")
    void testMultiByteCharacters(Charset charset) {
        assumeFalse(charset.equals(StandardCharsets.US_ASCII), "US_ASCII cannot encode multi-byte characters");

        Page page = new Page(512);
        String original = "こんにちは";
        int offset = 0;

        page.setString(offset, original, charset);
        String retrieved = page.getString(offset, charset);

        assertEquals(original, retrieved);
    }

    @ParameterizedTest(name = "adjacent strings don't bleed [{0}]")
    @MethodSource("supportedCharsets")
    @DisplayName("Two adjacent strings are read back independently without corruption")
    void testAdjacentStringsRoundTrip(Charset charset) {
        Page page = new Page(512);
        String first = "hello";
        String second = "world";

        int firstOffset = 0;
        int secondOffset = Page.calcMaxByteLength(first.length(), charset);

        page.setString(firstOffset, first, charset);
        page.setString(secondOffset, second, charset);

        assertEquals(first, page.getString(firstOffset, charset));
        assertEquals(second, page.getString(secondOffset, charset));
    }

    @ParameterizedTest(name = "calcMaxByteLength is sufficient [{0}]")
    @MethodSource("supportedCharsets")
    @DisplayName("calcMaxByteLength always allocates enough space for actual encoded bytes")
    void testCalcMaxByteLengthIsSufficient(Charset charset) {
        String str = "Test string for byte length";
        int maxBytes = Page.calcMaxByteLength(str.length(), charset);
        int actualBytes = Integer.BYTES + str.getBytes(charset).length;

        assertTrue(maxBytes >= actualBytes,
                "calcMaxByteLength returned " + maxBytes + " but actual encoded size is " + actualBytes);
    }
}