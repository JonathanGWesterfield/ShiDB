package File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.io.File;

public class FileMgr {

    private HashMap<String, RandomAccessFile> openFiles;
    private File dbDirectory;
    private int blockSize;
    private boolean isNew;

    public FileMgr(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;

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

    public void read(BlockId blk, Page page) {

    }

    public void write(BlockId blk, Page page) {

    }

    public BlockId append(String filename) {
        return null;
    }

    public boolean isNew() {
        return false;
    }

    public int length(String filename) {
        return 0;
    }

    public int blockSize() {
        return 0;
    }

    /**
     * Opens a new file for each table in the database.
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
            File dbTable = new File(dbDirectory, filename);
            file = new RandomAccessFile(dbTable, "rws");

            openFiles.put(filename, file);
        }

        return file;
    }
}
