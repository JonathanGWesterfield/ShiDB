package Buffer;

import static Constants.ShiDBModules.BUFFER;

import Startup.ShiDB;

public class BufferTest {
    public static void main(String[] args) {
        String testFileDir = "BufferTest";
        String logFilename = "bufferTestLogFile";

        try {
            ShiDB shiDB = new ShiDB(BUFFER, 400)
                    .bufferSize(3);

            BufferMgr bufferMgr = shiDB.getBufferMgr();

            // TODO: FINISH THIS
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
}
