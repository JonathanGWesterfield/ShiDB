package server;

import buffer.BufferMgr;
import file.FileMgr;
import log.LogMgr;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.IOException;

public class ShiDB {
    public static int BLOCK_SIZE = 400;
    public static int BUFFER_SIZE = 8;

    public static String LOG_FILE = "shidb-2.0.log";

    @Getter
    private FileMgr fileMgr;

    @Getter
    private LogMgr logMgr;

    @Getter @Setter
    private BufferMgr bufferMgr;


    /**
     * A constructor useful for debugging.
     *
     * @param dirName Where all the database files should live
     * @param blockSize Number of bytes each block in the database file should hold
     */
    public ShiDB(String dirName, int blockSize) throws IOException {
        File dbDirectory = new File(dirName);
        this.fileMgr = new FileMgr(dbDirectory, blockSize);
        this.logMgr = new LogMgr(fileMgr, LOG_FILE);
    }

    /**
     *
     * @param dirName Where all the database files should live
     * @param blockSize Number of bytes each block in the database file should hold
     * @param bufferSize Number of buffers/pages for the buffer manager to create, own, and manage
     */
    public ShiDB(String dirName, int blockSize, int bufferSize) throws IOException{
        File dbDirectory = new File(dirName);
        this.fileMgr = new FileMgr(dbDirectory, blockSize);
        this.logMgr = new LogMgr(fileMgr, LOG_FILE);
        this.bufferMgr = new BufferMgr(fileMgr, logMgr, bufferSize);
    }
}