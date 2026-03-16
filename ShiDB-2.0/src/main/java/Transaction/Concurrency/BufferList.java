package Transaction.Concurrency;

import Buffer.Buffer;
import Buffer.BufferMgr;
import File.BlockId;

import java.util.*;

public class BufferList {
    private Map<BlockId, Buffer> bufferLUT = new HashMap<>();

    private Set<BlockId> pins = new HashSet<>();

    private BufferMgr bufferMgr;

    public BufferList(BufferMgr bufferMgr) {
        this.bufferMgr = bufferMgr;
    }

    public Buffer getBuffer(BlockId block) {
        return bufferLUT.get(block);
    }

    public void pin(BlockId block) {
        Buffer buffer = bufferMgr.pinBuffer(block);
        bufferLUT.put(block, buffer);
        pins.add(block);
    }

    public void unpin(BlockId block) {
        Buffer buffer = bufferLUT.get(block);
        bufferMgr.unpinBuffer(buffer);
        pins.remove(block);
        if(!pins.contains(block))
            bufferLUT.remove(block);
    }

    public void unpinAll() {
        for (BlockId block : pins) {
            Buffer buffer = bufferLUT.get(block);
            bufferMgr.unpinBuffer(buffer);
        }

        bufferLUT.clear();
        pins.clear();
    }
}
