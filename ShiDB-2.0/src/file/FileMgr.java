package file;

import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class FileMgr {
    private File dbDirectory;

    @Getter
    private int blocksize;

    @Getter
    private boolean isNew;

    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileMgr(File dbDirectory, int blocksize) throws IOException {
        this.dbDirectory = dbDirectory;
        this.blocksize = blocksize;

        isNew = !dbDirectory.exists();

        // Create the database directory structure if it's brand new
        if (isNew)
            if (!dbDirectory.mkdirs())
                throw new IOException("Failed to create the needed database directory structure for a fresh dataabase!");

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
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot read block: " + block);
        }
    }

    public synchronized void writePageToDisk(BlockId block, Page page) {
        try {
            RandomAccessFile accessFile = fetchFile(block.filename());
            accessFile.seek(block.blockNum() * blocksize);

            accessFile.getChannel().write(page.getContents());
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

    private RandomAccessFile fetchFile(String filename) throws IOException {
        RandomAccessFile accessFile = openFiles.get(filename);

        if (accessFile == null) {
            File dbTable = new File(dbDirectory, filename);
            accessFile = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, accessFile);
        }
        return accessFile;
    }
}
