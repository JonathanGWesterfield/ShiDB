package Transaction;

import Buffer.BufferMgr;
import File.BlockId;
import File.FileMgr;
import Log.LogMgr;


// TODO: Implement the functions
// TODO: Implement the extra functions like getLong and getDatetime
public abstract class Transaction {

    public Transaction(FileMgr fileMgr, LogMgr logMgr, BufferMgr bufferMgr) {
        return;
    }

    abstract void commit();

    abstract void rollback();

    abstract void recover();

    abstract void pin(BlockId blk);

    abstract void unpin(BlockId blk);

    abstract int getInt(BlockId blk);

    abstract String getString(BlockId blk);

    abstract void setInt(BlockId blk);

    abstract  void setString(BlockId blk);

    abstract int getNumAvailableBuffers();

    abstract int size();

    abstract BlockId append(String filename);

    abstract int blockSize();
}
