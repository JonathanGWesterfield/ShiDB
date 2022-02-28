package Constants;

public enum TestConstants {
    FILE_DIR("_Test_File_Dir"),
    DB_FILE("_Test_DB_File.txt"),
    LOG_FILE("_Test_Log_File.txt"),
    TEST_STR("Big Fat Test String"); // str len = 19

    private final String value;

    TestConstants(String moduleTestName) {
        this.value = moduleTestName;
    }

    @Override
    public String toString() {
        return value;
    }
}
