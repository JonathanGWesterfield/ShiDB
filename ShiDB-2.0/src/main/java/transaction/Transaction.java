package transaction;

import buffer.BufferMgr;
import file.BlockId;
import file.FileMgr;
import log.LogMgr;
import lombok.extern.java.Log;

public abstract class Transaction {
    public Transaction() {};

    public Transaction(FileMgr fileMgr, LogMgr logMgr, BufferMgr bufferMgr) {};

    public void commit() {};

    public void rollback() {};

    public void recover() {};

    public void pin(BlockId block) {};

    public abstract int getInt(BlockId block, int offset);

    public abstract String getString(BlockId block, int offset);

    public abstract int setInt(BlockId blockId, int offset, int value, ShouldLog shouldLog);

    public abstract String setString(BlockId block, int offset, int value, ShouldLog shouldLog);

    public abstract int size(String filename);

    public abstract BlockId append(String filename);

    public abstract int blockSize();

}
