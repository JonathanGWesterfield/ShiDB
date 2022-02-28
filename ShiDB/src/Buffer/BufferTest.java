package Buffer;

import static Constants.ShiDBModules.BUFFER;

import java.util.concurrent.atomic.AtomicInteger;

import File.BlockId;
import File.Page;
import Startup.ShiDB;

public class BufferTest {
    public static void main(String[] args) {
        String testDBFileName = ShiDB.constructDBFileName(BUFFER);

        try {
            ShiDB shiDB = new ShiDB(BUFFER, 400)
                    .bufferSize(3);

            BufferMgr bufferMgr = shiDB.getBufferMgr();

            Buffer buffer1 = bufferMgr.pin(new BlockId(testDBFileName, 1));
            Page page1 = buffer1.getContents();
            int testNum = page1.getInt(80);

            page1.setInt(80, testNum + 1);
            buffer1.setModified(1, 0); // this modification will get written to the disk
            System.out.println("The new value of Page 1 is : " + (testNum + 1));

            bufferMgr.unPin(buffer1);

            // One of these pins will flush buffer1 to the disk:
            Buffer buffer2 = bufferMgr.pin(new BlockId(testDBFileName, 2));
            Buffer buffer3 = bufferMgr.pin(new BlockId(testDBFileName, 3));
            Buffer buffer4 = bufferMgr.pin(new BlockId(testDBFileName, 4));

            bufferMgr.unPin(buffer2);

            buffer2 = bufferMgr.pin(new BlockId(testDBFileName, 1));
            Page page2 = buffer2.getContents();

            page2.setInt(80, 9999); // this modification won't get written to the disk
            buffer2.setModified(1, 0);

            bufferMgr.unPin(buffer2);
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
}
