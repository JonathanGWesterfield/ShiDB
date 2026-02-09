package server;

import file.FileMgr;
import lombok.Getter;

import java.io.File;
import java.io.IOException;

public class ShiDB {
    public static int BLOCK_SIZE = 400;
    public static int BUFFER_SIZE = 8;
    public static String LOG_FILE = "simpledb.log";

    @Getter
    private FileMgr fileMgr;


    /**
     * A constructor useful for debugging.
     *
     * @param dirname   the name of the database directory
     * @param blocksize the block size
     */
    public ShiDB(String dirname, int blocksize) throws IOException {
        File dbDirectory = new File(dirname);
        this.fileMgr = new FileMgr(dbDirectory, blocksize);
    }
}