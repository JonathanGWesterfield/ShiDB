package File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

public class FileMgr {

    private HashMap<String, RandomAccessFile> openFiles;
    private File dbDirectory;
    private int blockSize;
    private boolean isNew;
    private AtomicLong numReads; // thread safe counting variables for read/write stats
    private AtomicLong numWrites;

    /**
     * Constructor. If the directory we will house our database is in doesn't exist
     * (the database is new), create the database directory. Will attempt to clean up
     * any temporary databases that were left behind from the last run.
     * @param dbDirectory The directory all of the database files (table, logs, etc.) will reside
     * @param blockSize The default blocksize that all blocks written to disks will be.
     */
    public FileMgr(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        this.numReads = new AtomicLong(0);
        this.numWrites = new AtomicLong(0);
        this.openFiles = new HashMap<>();

        // Need to check directory exists. Create if doesn't exist
        isNew = !dbDirectory.exists();

        if (isNew)
            dbDirectory.mkdir();

        // clean up temporary tables from the last run
        for (String filename : dbDirectory.list()) {
            if (filename.startsWith("temp"))
                new File(filename).delete();
        }
    }

    /**
     * Reads data from a specific block on the disk (table file) into a page for
     * further use.
     * @param blk The block where our information on the disk is.
     * @param page The page we want to load the information into.
     */
    public void read(BlockId blk, Page page) {
        try {
            RandomAccessFile file = getFile(blk.getFilename());
            file.seek(blk.getBlkNum() * blockSize);
            file.getChannel().read(page.contents());
            this.numReads.incrementAndGet();
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot read block: " + blk.getBlkNum() + "\nBlock: " + blk);
        }
    }

    /**
     * Writes data from a page into a specific block on the disk (table file).
     * @param blk The block on the disk we want to write data to.
     * @param page The memory page containing the data we want to write.
     */
    public void write(BlockId blk, Page page) {
        try {
            RandomAccessFile file = getFile(blk.getFilename());
            file.seek(blk.getBlkNum() * blockSize);
            file.getChannel().write(page.contents());
            this.numWrites.incrementAndGet();
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot write to block: " + blk.getBlkNum() + "\nBlock: " + blk);
        }
    }

    /**
     * Extends the file by 1 block on the disk. Does this by appending an empty byte array
     * of 1 block size to the end of the file. Relies on OS to extend file when we
     * force a new empty byte buffer to the end of it.
     * @param filename The file that needs to be appended/extended.
     * @return The new block at the end of the file.
     */
    public BlockId append(String filename) {
        BlockId newBlk = null;
        String err = "Cannot extend file: " + filename;

        try {
            int blkNum = eofBlockNum(filename);
            newBlk = new BlockId(filename, blkNum);

            RandomAccessFile file = getFile(newBlk.getFilename());
            file.seek(newBlk.getBlkNum() * blockSize); // seek to end of file
            byte[] emptyArr = new byte[blockSize]; // empty buffer to extend file with
            file.write(emptyArr);
        }
        catch (IOException e) {
            throw new RuntimeException(err);
        }

        if (newBlk == null)
            throw new RuntimeException(err);
        return newBlk;
    }

    public boolean isNew() {
        return this.isNew;
    }

    /**
     * Gives the block number at the very end of the file. Used for extending and appending
     * to table file.
     * @param filename Filename of the file we need to extend.
     * @return The block number to extend from.
     * @throws IOException
     */
    public int eofBlockNum(String filename) throws IOException {
        RandomAccessFile file = getFile(filename);
        return (int)(file.length() / blockSize);
    }

    /**
     * Opens a new file (tables, logs, etc) in the database.
     * @param filename Name of the file to be opened for the table
     * @return Sends back a file for reading and writing synchronously.
     * @throws IOException
     */
    private RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile file = openFiles.get(filename);

        // file hasn't been opened yet
        // doesn't mean file doesn't exist
        if (file == null) {
            // file for every table
            File filePtr = new File(dbDirectory, filename);
            file = new RandomAccessFile(filePtr, "rws");

            openFiles.put(filename, file);
        }

        return file;
    }

    /**
     * Gets the number of read operations the file manager has performed since startup.
     * @return The number of read operations
     */
    public long getNumReads() {
        return this.numReads.get();
    }

    /**
     * Gets the number of write operations the file manager has performed since startup.
     * @return The number of write operations
     */
    public long getNumWrites() {
        return this.numWrites.get();
    }

    /**
     * Gives the default blocksize the file manager has been set to (the number of elements
     * the byte buffer of each page or block on the disk can hold).
     * @return The default page block size.
     */
    public int getBlockSize() {
        return blockSize;
    }
}
