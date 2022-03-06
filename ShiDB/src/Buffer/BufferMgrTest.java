package Buffer;

import static Constants.ShiDBModules.BUFFER_MANAGER;

import File.BlockId;
import Startup.ConfigFetcher;
import Startup.ShiDB;
import Error.BufferAbortException;

public class BufferMgrTest {

    // TODO: Add lsn's to the buffers to test the LRU manager
    public static void main(String[] args) {
        String testDBFileName = ShiDB.constructDBFileName(BUFFER_MANAGER);

        ShiDB shiDB = new ShiDB(BUFFER_MANAGER, 400)
                .bufferSize(3).init();

        BufferMgr bufferMgr = shiDB.getBufferMgr();
        System.out.printf("Starting Buffer Manager Test using the %s replacement strategy!\n\n", ConfigFetcher.getConfigs().getBufferMgrReplacementStrategy());

        Buffer[] buffArr = new Buffer[6];
        System.out.println("Pinning Buffer: 0...");
        buffArr[0] = bufferMgr.pin(new BlockId(testDBFileName, 0));
        System.out.printf("Available Buffers: %d\n\n", bufferMgr.numAvailableBuffers());

        System.out.println("Pinning Buffer: 1...");
        buffArr[1] = bufferMgr.pin(new BlockId(testDBFileName, 1));
        System.out.printf("Available Buffers: %d\n\n", bufferMgr.numAvailableBuffers());

        System.out.println("Pinning Buffer: 2...");
        buffArr[2] = bufferMgr.pin(new BlockId(testDBFileName, 2));
        System.out.printf("Available Buffers: %d\n\n", bufferMgr.numAvailableBuffers());

        System.out.printf("Mock Modifying Buffer: 1...\n");
        buffArr[1].setModified(10234, 200);

        System.out.println("Unpinning Buffer 1...");
        bufferMgr.unPin(buffArr[1]);
        System.out.printf("Available Buffers: %d\n\n", bufferMgr.numAvailableBuffers());
        buffArr[1] = null;

        System.out.println("Pinning Buffer: 0...");
        buffArr[3] = bufferMgr.pin(new BlockId(testDBFileName, 0));
        System.out.printf("Available Buffers: %d\n\n", bufferMgr.numAvailableBuffers());

        System.out.println("Pinning Buffer: 1...");
        buffArr[4] = bufferMgr.pin(new BlockId(testDBFileName, 1));
        System.out.printf("Available Buffers: %d\n\n", bufferMgr.numAvailableBuffers());

        try {
            System.out.println("Attempting to pin block 3...\n");
            buffArr[5] = bufferMgr.pin(new BlockId(testDBFileName, 3));
        }
        catch (BufferAbortException e) {
            System.out.println(e.getMessage() + "\n");
        }
        catch (RuntimeException e) {
            System.out.println("RUNTIME EXCEPTION: " + e.getMessage());
        }

        System.out.printf("Mock Modifying Buffer: 2...\n");
        buffArr[2].setModified(243324, 300);

        System.out.println("Couldn't pin block 3 because no available buffers. Unpinning a buffer\n");
        bufferMgr.unPin(buffArr[2]);
        buffArr[5] = bufferMgr.pin(new BlockId(testDBFileName, 3)); // now this time, it will work

        System.out.printf("Mock Modifying Buffer: 4...\n");
        buffArr[4].setModified(10235, 400);

        System.out.printf("Unpinning buffer: 4...\n");
        bufferMgr.unPin(buffArr[4]);

        System.out.println("Final Buffer Allocation: ");
        for(int i = 0; i < buffArr.length; i++) {
            Buffer buff = buffArr[i];
            if (buff != null)
                System.out.printf("Buffer[%d] pinned to block: %s\n", i, buff.getBlock().toString());
        }
    }
}
