package Server;

import Buffer.*;
import File.FileMgr;
import Log.LogMgr;
import lombok.Getter;
import lombok.Setter;
import Transaction.Transaction;
import Transaction.Concurrency.TransactionRegistrySingleton;

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

        String logFilename = ConfigFetcher.getDBLogFileName();
        this.logMgr = new LogMgr(fileMgr, logFilename);
    }

    /**
     *
     * @param dirName Where all the database files should live
     * @param blockSize Number of bytes each block in the database file should hold
     * @param bufferSize Number of buffers/pages for the buffer manager to create, own, and manage
     */
    public ShiDB(String dirName, int blockSize, int bufferSize) throws IOException {
        File dbDirectory = new File(dirName);
        boolean isNewDatabase = !dbDirectory.exists();

        this.fileMgr = new FileMgr(dbDirectory, blockSize);
        this.logMgr = new LogMgr(fileMgr, ConfigFetcher.getDBLogFileName());
        this.bufferMgr = getCorrectBufferMgr(null, bufferSize);

        startDBRecovery(isNewDatabase);

        this.txRegistry = TransactionRegistrySingleton.getInstance();
        txRegistry.setLogMgr(logMgr);
    }

    /**
     * THIS IS THE MAIN CONSTRUCTOR THE SHIDB. ALL OTHERS ARE USED FOR UNIT TESTS
     * This fully utilizes the config to set itself up
      */
    public ShiDB() throws IOException {
        File dbDirectory = new File(ConfigFetcher.getDBFileDirectory());
        boolean isNewDatabase = !dbDirectory.exists();

        int fileBlockSize = ConfigFetcher.getDBFileBlockSize();
        this.fileMgr = new FileMgr(dbDirectory, fileBlockSize);
        this.logMgr = new LogMgr(fileMgr, ConfigFetcher.getDBLogFileName());
        this.bufferMgr = getCorrectBufferMgr(null, null);

        this.txRegistry = TransactionRegistrySingleton.getInstance();
        txRegistry.setLogMgr(logMgr);

        startDBRecovery(isNewDatabase);
    }

    // We need to ensure that database recover is run on startup regardless of a crash or not. We can skip recovery
    // if this database is literally brand new since no transactions have ever been made
    private void startDBRecovery(boolean isNewDatabase) {
        if (isNewDatabase)
            return;

        Transaction recoveryTx = new Transaction(fileMgr, logMgr, bufferMgr);
        recoveryTx.recover();
        recoveryTx.commit();
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
        this.logMgr = new LogMgr(fileMgr, ConfigFetcher.getDBLogFileName());
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