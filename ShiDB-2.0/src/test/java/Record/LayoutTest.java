package Record;

import File.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LayoutTest {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** The in-use flag written before every slot. Layout uses Byte.BYTES (1 byte) for this flag. */
    private static final int FLAG_BYTES = Byte.BYTES;

    /**
     * Mirrors Page.calcMaxByteLength so expected values stay in sync with the
     * production code without duplicating the charset assumption as a magic number.
     */
    private static int maxVarcharBytes(int length) {
        return Page.calcMaxByteLength(length);
    }

    // =========================================================================
    @Nested
    @DisplayName("Constructor: computed offsets")
    class ComputedOffsets {

        @Test
        @DisplayName("First field starts after the in-use flag (INTEGER_BYTES offset)")
        void firstFieldStartsAfterFlag() {
            Schema schema = new Schema();
            schema.addIntField("A");

            Layout layout = new Layout(schema);

            assertEquals(FLAG_BYTES, layout.getFieldOffset("A"),
                    "First field must be placed immediately after the in-use flag");
        }

        @Test
        @DisplayName("Fields are laid out contiguously in insertion order")
        void fieldsAreContiguous() {
            Schema schema = new Schema();
            schema.addIntField("A");
            schema.addStringField("B", 9);

            Layout layout = new Layout(schema);

            int expectedA = FLAG_BYTES;
            int expectedB = expectedA + Integer.BYTES;  // A is 4 bytes

            assertEquals(expectedA, layout.getFieldOffset("A"));
            assertEquals(expectedB, layout.getFieldOffset("B"));
        }

        @Test
        @DisplayName("All supported field types produce correct offsets")
        void allFieldTypesOffsets() {
            Schema schema = new Schema();
            schema.addIntField("intField");
            schema.addBooleanField("boolField");
            schema.addLongField("longField");
            schema.addDoubleField("doubleField");
            schema.addDateTimeField("timestampField");
            schema.addStringField("varcharField", 10);

            Layout layout = new Layout(schema);

            int pos = FLAG_BYTES;

            assertEquals(pos, layout.getFieldOffset("intField"));
            pos += Integer.BYTES;

            assertEquals(pos, layout.getFieldOffset("boolField"));
            pos += Byte.BYTES;

            assertEquals(pos, layout.getFieldOffset("longField"));
            pos += Long.BYTES;

            assertEquals(pos, layout.getFieldOffset("doubleField"));
            pos += Double.BYTES;

            assertEquals(pos, layout.getFieldOffset("timestampField"));
            pos += Long.BYTES;

            assertEquals(pos, layout.getFieldOffset("varcharField"));
        }

        @Test
        @DisplayName("VARCHAR offset accounts for Page.calcMaxByteLength, not raw character count")
        void varcharOffsetUsesMaxByteLength() {
            Schema schema = new Schema();
            schema.addStringField("str", 5);
            schema.addIntField("after");

            Layout layout = new Layout(schema);

            int expectedAfter = FLAG_BYTES + maxVarcharBytes(5);
            assertEquals(expectedAfter, layout.getFieldOffset("after"),
                    "Field after a VARCHAR must account for the full max byte width, not just character count");
        }

        @Test
        @DisplayName("slotSize is set by computed constructor — not zero")
        void slotSizeIsNonZeroAfterComputation() {
            Schema schema = new Schema();
            schema.addIntField("A");
            schema.addStringField("B", 9);

            Layout layout = new Layout(schema);

            assertNotEquals(0, layout.getSlotSize(),
                    "slotSize must be set by the computed constructor — a zero slotSize causes " +
                            "getSlotOffset() to return 0 for every slot, corrupting all record page logic");
        }

        @Test
        @DisplayName("slotSize equals flag bytes plus sum of all field byte lengths")
        void slotSizeEqualsExpectedTotal() {
            Schema schema = new Schema();
            schema.addIntField("A");
            schema.addStringField("B", 9);

            Layout layout = new Layout(schema);

            int expected = FLAG_BYTES + Integer.BYTES + maxVarcharBytes(9);
            assertEquals(expected, layout.getSlotSize(),
                    "slotSize must be flag + all field sizes so RecordPage can correctly " +
                            "compute slot boundaries and detect end of block");
        }
    }

    // =========================================================================
    @Nested
    @DisplayName("Constructor: direct injection (schema + offsets + slotSize)")
    class DirectInjectionConstructor {

        @Test
        @DisplayName("Injected offsets and slotSize are returned as-is")
        void injectedValuesArePreserved() {
            Schema schema = new Schema();
            schema.addIntField("X");

            Map<String, Integer> offsets = new HashMap<>();
            offsets.put("X", 99);

            Layout layout = new Layout(schema, offsets, 128);

            assertEquals(99, layout.getFieldOffset("X"));
            assertEquals(128, layout.getSlotSize());
        }

        @Test
        @DisplayName("Injected layout does not recompute offsets")
        void injectedLayoutDoesNotRecompute() {
            // Deliberately wrong offset — if Layout recomputed it would be FLAG_BYTES (4), not 42.
            Schema schema = new Schema();
            schema.addIntField("Y");

            Map<String, Integer> offsets = Map.of("Y", 42);
            Layout layout = new Layout(schema, offsets, 64);

            assertEquals(42, layout.getFieldOffset("Y"),
                    "Direct-injection constructor must not recompute offsets");
        }
    }

    // =========================================================================
    @Nested
    @DisplayName("fieldByteLength")
    class FieldByteLength {

        @Test
        @DisplayName("INTEGER -> Integer.BYTES")
        void integerByteLength() {
            Schema schema = new Schema();
            schema.addIntField("f");
            assertEquals(Integer.BYTES, new Layout(schema).fieldByteLength("f"));
        }

        @Test
        @DisplayName("BOOLEAN -> Byte.BYTES")
        void booleanByteLength() {
            Schema schema = new Schema();
            schema.addBooleanField("f");
            assertEquals(Byte.BYTES, new Layout(schema).fieldByteLength("f"));
        }

        @Test
        @DisplayName("BIGINT -> Long.BYTES")
        void bigintByteLength() {
            Schema schema = new Schema();
            schema.addLongField("f");
            assertEquals(Long.BYTES, new Layout(schema).fieldByteLength("f"));
        }

        @Test
        @DisplayName("DOUBLE -> Double.BYTES")
        void doubleByteLength() {
            Schema schema = new Schema();
            schema.addDoubleField("f");
            assertEquals(Double.BYTES, new Layout(schema).fieldByteLength("f"));
        }

        @Test
        @DisplayName("TIMESTAMP -> Long.BYTES")
        void timestampByteLength() {
            Schema schema = new Schema();
            schema.addDateTimeField("f");
            assertEquals(Long.BYTES, new Layout(schema).fieldByteLength("f"));
        }

        @Test
        @DisplayName("VARCHAR -> Page.calcMaxByteLength(length)")
        void varcharByteLength() {
            Schema schema = new Schema();
            schema.addStringField("f", 20);
            assertEquals(maxVarcharBytes(20), new Layout(schema).fieldByteLength("f"));
        }

        @Test
        @DisplayName("VARCHAR(0) is valid and produces a non-negative byte length")
        void varcharZeroLength() {
            Schema schema = new Schema();
            schema.addStringField("f", 0);
            assertTrue(new Layout(schema).fieldByteLength("f") >= 0);
        }
    }

    // =========================================================================
    @Nested
    @DisplayName("getFieldOffset: error handling")
    class GetFieldOffsetErrors {

        @Test
        @DisplayName("Throws when field does not exist in layout")
        void throwsForMissingField() {
            Layout layout = new Layout(new Schema());

            assertThrows(RuntimeException.class, () -> layout.getFieldOffset("nonexistent"),
                    "Should throw for a field not present in the layout");
        }
    }

    // =========================================================================
    @Nested
    @DisplayName("Schema passthrough")
    class SchemaPassthrough {

        @Test
        @DisplayName("getSchema() returns the exact schema passed to the constructor")
        void schemaIsPreserved() {
            Schema schema = new Schema();
            schema.addIntField("A");

            Layout layout = new Layout(schema);

            assertSame(schema, layout.getSchema());
        }

        @Test
        @DisplayName("Schema field list is consistent with layout offsets")
        void schemaFieldsMatchOffsetKeys() {
            Schema schema = new Schema();
            schema.addIntField("A");
            schema.addStringField("B", 5);
            schema.addBooleanField("C");

            Layout layout = new Layout(schema);

            List<String> schemaFields = layout.getSchema().getFields();
            for (String field : schemaFields) {
                // Should not throw — every schema field must have an offset entry
                assertDoesNotThrow(() -> layout.getFieldOffset(field),
                        "Field '" + field + "' in schema has no corresponding offset in layout");
            }
        }
    }
}