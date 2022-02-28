package Startup;

import Buffer.BufferMgr;
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
    public static BufferMgr getCorrectBuffer(FileMgr fileMgr, LogMgr logMgr, int numBuffers) throws IOException {
        BufferMgrReplacementStrategies replacementStrategy = Config.getConfigs().getBufferMgrReplacementStrategy();
        BufferMgr bufferMgr = null;

        switch(replacementStrategy) {
        case LRU:
            bufferMgr = new LRUBufferMgr(fileMgr, logMgr, numBuffers);
            break;
        case FIFO:
        case CLOCK:
        default:
            bufferMgr = new BufferMgr(fileMgr, logMgr, numBuffers); // default niave replacement strategy
        }

        return bufferMgr;
    }
}
