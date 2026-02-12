package file;

import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** TODO:
 * I didn't realize that java.io.File was so outdate and modern libraries have replaced it. I need to refactor this
 * file to use the new java.nio.Files and java.nio.Path classes
 *
 * Maybe also need to replace RandomAccessFile with the FileChannel + SeekableByteChannel combo
 *
 * Reference:
 * https://grok.com/share/c2hhcmQtNA_1e6aa0a9-8fdf-469c-b655-831ea840b398
 */

public class FileMgr {
    private File dbDirectory;

    @Getter
    private int blocksize;

    @Getter
    private boolean isNew;

    // Can't autogenerate lombok getter function because it doesn't call AtomicInteger.get()
    private AtomicLong blocksWriteCounter;
    private AtomicLong blocksReadCounter;

    @Getter
    private ConcurrentHashMap<String, Integer> numFilesAppended;

    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public long getBlocksWriteCounter() {
        return blocksWriteCounter.get();
    }

    public long getBlocksReadCounter() {
        return blocksReadCounter.get();
    }

    public int getNumAppends(String filename) {
        if (!numFilesAppended.containsKey(filename))
            throw new RuntimeException("Filename " + filename + " has no associated appends!");

        return numFilesAppended.get(filename);
    }

    public FileMgr(File dbDirectory, int blocksize) throws IOException {
        this.dbDirectory = dbDirectory;
        this.blocksize = blocksize;

        // Initialize the statistics for the file manager
        this.blocksReadCounter = new AtomicLong(0);
        this.blocksWriteCounter = new AtomicLong(0);
        this.numFilesAppended = new ConcurrentHashMap<>();

        isNew = !dbDirectory.exists();

        // Create the database directory structure if it's brand new
        if (isNew)
            if (!dbDirectory.mkdirs())
                throw new IOException("Failed to create the needed database directory structure for a fresh database!");

        for (String filename : dbDirectory.list())
            if (filename.startsWith("temp"))
                new File(dbDirectory, filename).delete();
    }

    public synchronized void readFromDiskToPage(BlockId block, Page page) {
        try {
            RandomAccessFile accessFile = fetchFile(block.filename());
            accessFile.seek(block.blockNum() * blocksize);

            // This will read the contents of the file into the page bytebuffer by reference
            // They say Java doesn't have pointers, but references are basically the same thing
            accessFile.getChannel().read(page.getContents());

            blocksReadCounter.incrementAndGet();
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot read block: " + block);
        }
    }

    // Helper function for unit tests. Startup of the database impacts the read/write statistics. Sections like the
    // logMgr also perform reads and writes on startup, so we need this helper to reset the counters for unit tests
    public void resetFileMgrStatistics() {
        this.blocksReadCounter = new AtomicLong(0);
        this.blocksWriteCounter = new AtomicLong(0);
        this.numFilesAppended = new ConcurrentHashMap<>();
    }

    public synchronized void writePageToDisk(BlockId block, Page page) {
        try {
            RandomAccessFile accessFile = fetchFile(block.filename());
            accessFile.seek(block.blockNum() * blocksize);

            accessFile.getChannel().write(page.getContents());

            blocksWriteCounter.incrementAndGet();
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot write block: " + block);
        }
    }

    public synchronized BlockId append(String filename) {
        int newBlockNum = numBlocksInFile(filename);
        BlockId newBlock = new BlockId(filename, newBlockNum);

        byte[] emptyByteArr = new byte[blocksize];

        try {
            RandomAccessFile accessFile = fetchFile(filename);
            accessFile.seek(newBlock.blockNum() * blocksize);
            accessFile.write(emptyByteArr);

            numFilesAppended.putIfAbsent(filename, 0);
            int numAppends = numFilesAppended.get(filename);
            numFilesAppended.put(filename, ++numAppends);
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot append block: " + newBlock);
        }

        return newBlock;
    }

    public int numBlocksInFile(String filename) {
        try {
            RandomAccessFile accessFile = fetchFile(filename);
            return (int) (accessFile.length() / blocksize);
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot access file when checking file length: " + filename);
        }
    }

    /**
     * This is a helper function for unit testing. This is an easy way to go through all of the files
     * opened during runtime, close the connections and delete them. Maybe it'll be used to drop tables
     * at some point, idk
     * @param filename
     */
    public void deleteFile(String filename) {
        try {
            // First need to ensure that the random access file is closed
            RandomAccessFile accessFile = fetchFile(filename);
            accessFile.close();

            new File(filename).delete();
        }
        catch (IOException e) {
            // Since this function is purely used to help with unit testing, don't care if deletion fails
            System.out.println("Failed to delete file: " + filename);
        }
    }

    /**
     * Fetches a file and returns the file handler. If no such file exists, one will be created and returned
     * @param filename Name of the file to fetch/create
     * @return accessFile File handler to use
     * @throws IOException
     */
    private RandomAccessFile fetchFile(String filename) throws IOException {
        if (openFiles.containsKey(filename))
            return openFiles.get(filename);

        File dbTable = new File(dbDirectory, filename);

        // RWS -> Read/Write/OS should not delay disk I/O to optimize disk performance
        // S ensures that every write operation must be written immediately to the disk
        RandomAccessFile accessFile = new RandomAccessFile(dbTable, "rws");
        openFiles.put(filename, accessFile);
        return accessFile;
    }
}
