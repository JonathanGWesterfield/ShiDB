package recovery;

import file.BlockId;
import file.Page;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import server.ShiDB;
import transaction.concurrency.ConcurrencyMgr;
import transaction.concurrency.TransactionRegistrySingleton;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public abstract class RecoveryMgrTestBase {

    protected static final String TEST_DIR = "recovery-mgr-test";
    protected static final String TEST_FILE = "testdata";
    protected static final int BLOCK_SIZE = 400;
    protected static final int BUFFER_SIZE = 8;
    protected static final int OFFSET = 0;
    protected static final int OFFSET_2 = Integer.BYTES;

    @BeforeEach
    void resetState() {
        ConcurrencyMgr.reinit();
        TransactionRegistrySingleton.getInstance().reinit();
    }

    @AfterEach
    void cleanUp() throws IOException {
        deleteDirectory(TEST_DIR);
    }

    /**
     * Initializes a block on disk with a zeroed page so transactions can pin and write to it.
     */
    protected BlockId initializeBlock(ShiDB db) throws IOException {
        BlockId blk = new BlockId(TEST_FILE, 0);
        Page page = new Page(db.getFileMgr().getBlocksize());
        page.setInt(OFFSET, 0);
        page.setInt(OFFSET_2, 0);
        db.getFileMgr().writePageToDisk(blk, page);
        return blk;
    }

    private void deleteDirectory(String dirName) throws IOException {
        Path path = Path.of(dirName);
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }
}