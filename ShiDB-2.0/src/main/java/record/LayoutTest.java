package record;

public class LayoutTest {
    public static void main(String[] args) throws Exception {
        Schema sch = new Schema();
        sch.addIntField("A");
        sch.addStringField("B", 9);
        Layout layout = new Layout(sch);
        for (String fieldName : layout.getSchema().getFields()) {
            int offset = layout.getFieldOffset(fieldName);
            System.out.println(fieldName + " has offset " + offset);
        }
    }
}
