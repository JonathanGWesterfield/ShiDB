package Query;

public class Constant {
    private Integer intVal = null;
    private String strVal = null;

    public Constant(Integer intVal) {
        this.intVal = intVal;
    }

    public Constant(String strVal) {
        this.strVal = strVal;
    }

    public int asInt() {
        return intVal;
    }

    public String asString() {
        return strVal;
    }

    public boolean equals(Object obj) {
        Constant constObj = (Constant) obj;
        return (intVal != null) ? intVal.equals(constObj.intVal) : strVal.equals(constObj.strVal);
    }

    public int compareTo(Constant constObj) {
        return (intVal != null) ? intVal.compareTo(constObj.intVal) : strVal.compareTo(constObj.strVal);
    }

    public int hashCode() {
        return (intVal != null) ? intVal.hashCode() : strVal.hashCode();
    }

    public String toString() {
        return (intVal != null) ? intVal.toString() : strVal;
    }
}
