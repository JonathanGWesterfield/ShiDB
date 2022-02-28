package Constants;

public enum ShiDBModules {
    /**
     * Module for the File Manager.
     */
    FILE,

    /**
     * Module for the Log Manager
     */
    LOG,

    /**
     * Module for the Buffer Manager
     */
    BUFFER,

    /**
     * Not the same as buffer. Buffer Manager module uses the Buffer
     */
    BUFFER_MANAGER,

    /**
     * The entire ShiDB system - all modules
     */
    SHIDB;
}
