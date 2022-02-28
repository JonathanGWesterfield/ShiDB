package Buffer;

import static Constants.ShiDBModules.BUFFER_MANAGER;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import File.BlockId;
import Startup.ShiDB;
import Error.BufferAbortException;

public class BufferMgrTest {
    public static void main(String[] args) {
        String testDBFileName = ShiDB.constructDBFileName(BUFFER_MANAGER);

        ShiDB shiDB = new ShiDB(BUFFER_MANAGER, 400)
                .bufferSize(3).init();

        BufferMgr bufferMgr = shiDB.getBufferMgr();

        Buffer[] buffArr = new Buffer[6];
        buffArr[0] = bufferMgr.pin(new BlockId(testDBFileName, 0));
        buffArr[1] = bufferMgr.pin(new BlockId(testDBFileName, 1));
        buffArr[2] = bufferMgr.pin(new BlockId(testDBFileName, 2));

        bufferMgr.unPin(buffArr[1]);
        buffArr[1] = null;

        buffArr[3] = bufferMgr.pin(new BlockId(testDBFileName, 0));
        buffArr[4] = bufferMgr.pin(new BlockId(testDBFileName, 1));

        System.out.println("Available Buffers: " + bufferMgr.numAvailableBuffers());

        try {
            System.out.println("Attempting to pin block 3...");
            buffArr[5] = bufferMgr.pin(new BlockId(testDBFileName, 3));
        }
        catch (BufferAbortException e) {
            System.out.println(e.getMessage());
        }
        catch (RuntimeException e) {
            System.out.println("RUNTIME EXCEPTION: " + e.getMessage());
        }

        System.out.println("Couldn't pin block 3 because no available buffers. Unpinning a buffer");
        bufferMgr.unPin(buffArr[2]);
        buffArr[5] = bufferMgr.pin(new BlockId(testDBFileName, 3)); // now this time, it will work

        System.out.println("Final Buffer Allocation: ");
        for(int i = 0; i < buffArr.length; i++) {
            Buffer buff = buffArr[i];
            if (buff != null)
                System.out.printf("Buffer[%d] pinned to block: %s\n", i, buff.getBlock().toString());
        }
    }
}
