package Record;

import File.Page;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.*;

public class Layout {
    @Getter
    private Schema schema;

    private Map<String, Integer> offsets;

    @Getter
    private int slotSize;

    public Layout(Schema schema) {
        this.schema = schema;
        offsets = new HashMap<>();
        int position = Byte.BYTES; // space for flag specifying if the record is in use or not
        for (String fieldName : schema.getFields()) {
            offsets.put(fieldName, position);
            position += fieldByteLength(fieldName);
        }

        slotSize = position;
    }

    public Layout(Schema schema, Map<String, Integer> offsets, int slotSize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotSize = slotSize;
    }

    public int getFieldOffset(String fieldName) {
        if (!offsets.containsKey(fieldName))
            throw new RuntimeException("Layout field " + fieldName + " does not exist!");
        return offsets.get(fieldName);
    }

    public int fieldByteLength(String fieldName) {
        int fieldType = getSchema().getFieldType(fieldName);
        return switch (fieldType) {
            case INTEGER -> Integer.BYTES;
            case BOOLEAN -> Byte.BYTES;
            case BIGINT -> Long.BYTES;
            case DOUBLE -> Double.BYTES;
            case TIMESTAMP -> Long.BYTES;
            case CHAR -> Byte.BYTES;
            case VARCHAR -> Page.calcMaxByteLength(getSchema().getFieldLength(fieldName));
            default -> throw new RuntimeException(fieldName + " has unrecognized SQL field type: " + fieldType);
        };
    }
}
