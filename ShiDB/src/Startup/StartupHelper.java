package Startup;

import Buffer.BufferMgr;
import Buffer.NaiveBufferMgr;
import Buffer.FIFOBufferMgr;
//import Buffer.LRUBufferMgr;
import Buffer.LRUBufferMgr;
import Constants.BufferMgrReplacementStrategies;
import File.FileMgr;
import Log.LogMgr;
import java.io.IOException;

public class StartupHelper {

    /**
     * Gets the correct bufferMgr with the replacement strategy specified in the configs file.
     * @param fileMgr File manager to read blocks from disk into pages
     * @param logMgr Log manager to log events in case of system crash.
     * @param numBuffers The size of the buffer pool.
     */
    public static BufferMgr getCorrectBufferMgr(FileMgr fileMgr, LogMgr logMgr, int numBuffers) throws IOException {
        BufferMgrReplacementStrategies replacementStrategy = ConfigFetcher.getConfigs().getBufferMgrReplacementStrategy();
        BufferMgr bufferMgr = null;

        switch(replacementStrategy) {
        case FIFO:
            bufferMgr = new FIFOBufferMgr(fileMgr, logMgr, numBuffers);
            break;
        case LRU:
            bufferMgr = new LRUBufferMgr(fileMgr, logMgr, numBuffers);
            break;
        default:
            bufferMgr = new NaiveBufferMgr(fileMgr, logMgr, numBuffers); // default niave replacement strategy
        }

        System.out.printf("Instantiating with the %s Buffer Manager!\n", replacementStrategy);

        return bufferMgr;
    }
}
