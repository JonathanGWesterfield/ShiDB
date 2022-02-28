package Buffer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import File.FileMgr;
import Log.LogMgr;

/**
 * Class for the buffer manager that uses the Least Recenty Used (LRU)
 * buffer replacement strategy.
 */
//public class LRUBufferMgr extends BufferMgr {
//
//    private Queue availableBuffers;
//
//    /**
//     * Constructor for buffer manager. Only 1 buffer manager is created
//     * during DB startup.
//     *
//     * @param fileMgr File manager to read blocks from disk into pages
//     * @param logMgr Log manager to log events in case of system crash.
//     * @param numBuffers The size of the buffer pool.
//     */
//    public LRUBufferMgr(FileMgr fileMgr, LogMgr logMgr, int numBuffers) {
//        super(fileMgr, logMgr, numBuffers);
//        availableBuffers = new LinkedList<Buffer>();
//    }
//
//    private void initEmptyBuffers() {
//        for (int i = 0; i < numAvailableBuffs.get(); i++)
//            availableBuffers.add(new Buffer(fileMgr, logMgr));
//    }
//}
