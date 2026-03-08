package server;

import buffer.*;
import file.FileMgr;
import log.LogMgr;
import lombok.Getter;
import lombok.Setter;
import transaction.concurrency.TransactionRegistrySingleton;

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

    @Getter @Setter
    private TransactionRegistrySingleton txRegistry;


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
    public ShiDB(String dirName, int blockSize, int bufferSize) throws IOException {
        File dbDirectory = new File(dirName);
        this.fileMgr = new FileMgr(dbDirectory, blockSize);
        this.logMgr = new LogMgr(fileMgr, LOG_FILE);
        this.bufferMgr = getCorrectBufferMgr(null, bufferSize);

        this.txRegistry = TransactionRegistrySingleton.getInstance();
        txRegistry.setLogMgr(logMgr);
    }

    /**
     * Constructor needed for unit tests that test the various buffer strategies
     * @param dirName
     * @param blockSize
     * @param bufferSize
     * @param bufferStrategy
     * @throws IOException
     */
    public ShiDB(String dirName, int blockSize, int bufferSize, BufferSelectionStrategy bufferStrategy) throws IOException {
        File dbDirectory = new File(dirName);
        this.fileMgr = new FileMgr(dbDirectory, blockSize);
        this.logMgr = new LogMgr(fileMgr, LOG_FILE);
        this.bufferMgr = getCorrectBufferMgr(bufferStrategy, bufferSize);
    }

    public BufferMgr getCorrectBufferMgr(BufferSelectionStrategy bufferStrategy, Integer bufferSize) {
        BufferSelectionStrategy strategy = bufferStrategy != null ?
                bufferStrategy : ConfigFetcher.getBufferMgrSelectionStrategy();

        int size = bufferSize != null ? bufferSize : ConfigFetcher.getSizeOfBufferPool();

        return switch (strategy) {
            case BufferSelectionStrategy.FIFO -> new FIFOBufferMgrStrategy(fileMgr, logMgr, size);
            case BufferSelectionStrategy.LRU -> new LRUBufferMgrStrategy(fileMgr, logMgr, size);
            case BufferSelectionStrategy.RING_BUFFER -> new RingBufferMgrStrategy(fileMgr, logMgr, size);
            default -> new NaiveBufferMgrStrategy(fileMgr, logMgr, size);
        };
    }
}