package Buffer;

import java.util.Stack;

import File.Page;
import File.FileMgr;
import File.BlockId;
import Log.LogMgr;

public class Buffer {

    private FileMgr fileMgr;
    private LogMgr logMgr;
    private Stack<Short> pins;

    public Buffer(FileMgr fileMgr, LogMgr logMgr) {

    }

    public Page contents() {
        return null;
    }

    public BlockId block() {
        return null;
    }

    public boolean isPinned() {
        return !pins.isEmpty();
    }

    public int setPinned() {
        if (pins == null)
            pins = new Stack<Short>();
        pins.push((short)1);
        return pins.size();
    }

    public int unPin() {
        if (!pins.isEmpty())
            pins.pop();
        return pins.size();
    }

    public void setModified(int txNum, int lsn) {

    }

    public int modifyingTx() {
        return 0;
    }
}