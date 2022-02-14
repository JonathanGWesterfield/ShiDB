package Startup;

import java.io.File;

import Constants.ShiDBModules;
import Buffer.BufferMgr;
import Constants.TestConstants;
import File.FileMgr;
import Log.LogMgr;

public class ShiDB {

    private int blockSize;
    private int bufferSize;

    private FileMgr fileMgr;
    private LogMgr logMgr;
    private BufferMgr bufferMgr;

    /*
    Constructor for actually setting the full server
     */
    public ShiDB() {}

    /**
     * Constructor for test instances depending on which test is being run. Will create a ShiDB
     * instance with the correct objects and data instantiated to do setup of the test.
     * @param module The specific module being tested
     * @param blockSize The default blocksize that all blocks written to disks will be
     */
    public ShiDB(ShiDBModules module, int blockSize) {
        this.blockSize = blockSize;

        try {
            switch (module) {
                case FILE:
                    constructFileTestShiDB();
                    break;
                case LOG:
                    constructLogTestShiDB();
                    break;
                case BUFFER:
                    constructBufferTestShiDB();
                    break;
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /**
     * Builder pattern method that allows setting the size of the BufferMgr
     * @param bufferSize number of buffers that the BufferMgr can hold
     * @return instance of this class
     */
    public ShiDB bufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    /**
     * Constructor to easily create a ShiDB instance for the File module test. Only
     * instantiates the modules necessary for that test.
     */
    private void constructFileTestShiDB() {
        fileMgr = new FileMgr(new File(constructTestFileDir(ShiDBModules.FILE)), blockSize);
    }

    /**
     * Constructor to easily create a ShiDB instance for the File module test. Only
     * instantiates the modules necessary for that test.
     */
    private void constructLogTestShiDB() throws Exception {
        fileMgr = new FileMgr(new File(constructTestFileDir(ShiDBModules.LOG)), blockSize);
        logMgr = new LogMgr(fileMgr, constructLogFileName(ShiDBModules.LOG));
    }

    /**
     * Constructor to easily create a ShiDB instance for the File module test. Only
     * instantiates the modules necessary for that test.
     */
    private void constructBufferTestShiDB() throws Exception {
        fileMgr = new FileMgr(new File(constructTestFileDir(ShiDBModules.BUFFER)), blockSize);
        logMgr = new LogMgr(fileMgr, constructLogFileName(ShiDBModules.BUFFER));
        bufferMgr = new BufferMgr(fileMgr, logMgr, bufferSize);
    }

    /**
     * Helper function to create the file directory for the specific test being run
     * @param module The module being tested
     * @return The name of the test directory
     */
    public static String constructTestFileDir(ShiDBModules module) {
        return module.toString() + TestConstants.FILE_DIR;
    }

    /**
     * Helper function to create the log file name for the specific test being run
     * @param module The module being tested
     * @return The name of the log file
     */
    public static String constructLogFileName(ShiDBModules module) {
        return module.toString() + TestConstants.LOG_FILE;
    }

    public FileMgr getFileMgr() {
        return fileMgr;
    }

    public LogMgr getLogMgr() {
        return logMgr;
    }

    public BufferMgr getBufferMgr() {
        return bufferMgr;
    }
}
