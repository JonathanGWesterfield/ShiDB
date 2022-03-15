package Transaction;

import static Constants.ShiDBModules.TRANSACTION_MANAGER;

import File.BlockId;
import Startup.ShiDB;

public class TransactionTest {
    public static void main(String[] args) {
//        String testDBFileName = ShiDB.constructDBFileName(TRANSACTION_MANAGER;
//
//        ShiDB shiDB = new ShiDB(TRANSACTION_MANAGER, 400)
//                .bufferSize(3)
//                .init();
//
//        Transaction tx1 = new Transaction(shiDB.getFileMgr(), shiDB.getLogMgr(), shiDB.getBufferMgr()){};
//
//        BlockId blk = new BlockId(testDBFileName, 1);
//        tx1.pin(blk);
//
//        // Don't log initial block values
//        tx1.setInt(blk, 80, 1, false);
//        tx1.setString(blk, 40, "one", false);
//        tx1.commit();
//
//        Transaction tx2 = new Transaction(shiDB.getFileMgr(), shiDB.getLogMgr(), shiDB.getBufferMgr()){};
//        tx2.pin(blk);
//
//        int intValue = tx2.getInt(blk, 80);
//        String stringValue = tx2.getString(blk, 40);
//
//        System.out.println("initial value at location 80 = " + intValue);
//        System.out.println("initial value at location 40 = " + stringValue);
//
//        int newIntValue = intValue + 1;
//        String newStringValue = stringValue + "!";
//
//        tx2.setInt(blk, 80, newIntValue, true);
//        tx2.setString(blk, 40, newStringValue, true);
//        tx2.commit();
//
//        Transaction tx3 = new Transaction(shiDB.getFileMgr(), shiDB.getLogMgr(), shiDB.getBufferMgr()){};
//        tx3.pin(blk);
//
//        System.out.println("new value at location 80 = " + tx3.getInt(blk, 80));
//        System.out.println("new value at location 40 = " + tx3.getString(blk, 40));
//
//        tx3.setInt(blk, 80, 9999, true);
//        System.out.println("pre-rollback value at location 80 = " + tx3.getInt(blk, 80));
//        tx3.rollback();
//
//        Transaction tx4 = new Transaction(shiDB.getFileMgr(), shiDB.getLogMgr(), shiDB.getBufferMgr()){};
//        tx4.pin(blk);
//
//        System.out.println("post-rollback at location 80 = " + tx4.getInt(blk, 80));
//        tx4.commit();
    }
}
