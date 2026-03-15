package record;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.sql.Types.*;

@Slf4j(topic = "RecordMgr")
public class Schema {
    private static final Map<Integer, String> sqlTypesMap = Map.of(
      INTEGER, "INTEGER",
      VARCHAR, "VARCHAR",
      BOOLEAN, "BOOLEAN",
      CHAR, "CHAR",
      BIGINT, "BIGINT",
      DOUBLE, "DOUBLE",
      TIMESTAMP, "TIMESTAMP"
    );

    @Getter
    private List<String> fields = new ArrayList<>();

    private Map<String, FieldInfo> info = new HashMap<>();

    public void addField(String fieldName, int type, int length) {
        // The length field is really only used for variable length fields like Strings (VARCHAR). It isn't used
        // for any fixed length values
        FieldInfo fInfo = new FieldInfo(type, length);
        fields.add(fieldName);
        info.put(fieldName, fInfo);
        log.debug("Adding field to schema: {} -> [type={}, length={}]",
                fieldName, typeToString(fInfo.type()), fInfo.length());
    }

    public static String typeToString(int sqlType) {
        if (sqlTypesMap.containsKey(sqlType))
            return sqlTypesMap.get(sqlType);

        throw new IllegalArgumentException("Unrecognized Java SQL type: " + sqlType);
    }

    public void addIntField(String fieldName) {
        addField(fieldName, INTEGER, Integer.BYTES);
    }

    public void addStringField(String fieldName, int length) {
        addField(fieldName, VARCHAR, length);
    }

    public void addBooleanField(String fieldName) {
        addField(fieldName, BOOLEAN, Byte.BYTES);
    }

    public void addByteField(String fieldName) {
        addField(fieldName, CHAR, Byte.BYTES);
    }

    public void addLongField(String fieldName) {
        addField(fieldName, BIGINT, Long.BYTES);
    }

    public void addDoubleField(String fieldName) {
        addField(fieldName, DOUBLE, Double.BYTES);
    }

    public void addDateTimeField(String fieldName) {
        // Since this is implemented as a long under the hood, I may need to change this to BIGINT later
        addField(fieldName, TIMESTAMP, Long.BYTES);
    }

    public void addFromSchema(String fieldName, Schema schema) {
        int type = schema.getFieldType(fieldName);
        int length = schema.getFieldLength(fieldName);
        addField(fieldName, type, length);
    }

    public void addAllFromSchema(Schema schema) {
        schema.fields.forEach((fieldName) -> addFromSchema(fieldName, schema));
    }

    public boolean schemaHasField(String fieldName) {
        return fields.contains(fieldName);
    }

    public int getFieldType(String fieldName) {
        checkFieldNameExists(fieldName);

        return info.get(fieldName).type();
    }

    public String fieldTypeToString(String fieldName) {
        int fieldType = getFieldType(fieldName);
        return typeToString(fieldType);
    }

    public int getFieldLength(String fieldName) {
        checkFieldNameExists(fieldName);

        return info.get(fieldName).length();
    }

    private void checkFieldNameExists(String fieldName) {
        if (schemaHasField(fieldName))
            return;

        throw new RuntimeException(String.format("Field %s does not exist in the schema!", fieldName));
    }
}
