package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.*;
import simpledb.file.Block;
import simpledb.buffer.Buffer;
import simpledb.server.SimpleDB;
import java.util.*;

/**
 * The recovery manager.  Each transaction has its own recovery manager.
 * @author Edward Sciore
 */
public class RecoveryMgr {
   private int txnum;

   /**
    * Creates a recovery manager for the specified transaction.
    * @param txnum the ID of the specified transaction
    */
   public RecoveryMgr(int txnum) {
      this.txnum = txnum;
      new StartRecord(txnum).writeToLog();
   }

   /**
    * Writes a commit simpledb.record to the simpledb.log, and flushes it to disk.
    */
   public void commit() {
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new CommitRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Writes a rollback simpledb.record to the simpledb.log, and flushes it to disk.
    */
   public void rollback() {
      doRollback();
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new RollbackRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Recovers uncompleted transactions from the simpledb.log,
    * then writes a quiescent checkpoint simpledb.record to the simpledb.log and flushes it.
    */
   public void recover() {
      doRecover();
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new CheckpointRecord().writeToLog();
      SimpleDB.logMgr().flush(lsn);

   }

   /**
    * Writes a setint simpledb.record to the simpledb.log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the simpledb.buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setInt(Buffer buff, int offset, int newval) {
      int oldval = buff.getInt(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetIntRecord(txnum, blk, offset, oldval).writeToLog();
   }

   /**
    * Writes a setstring simpledb.record to the simpledb.log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the simpledb.buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setString(Buffer buff, int offset, String newval) {
      String oldval = buff.getString(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetStringRecord(txnum, blk, offset, oldval).writeToLog();
   }

   /**
    * Rolls back the transaction.
    * The method iterates through the simpledb.log records,
    * calling undo() for each simpledb.log simpledb.record it finds
    * for the transaction,
    * until it finds the transaction's START simpledb.record.
    */
   private void doRollback() {
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         if (rec.txNumber() == txnum) {
            if (rec.op() == START)
               return;
            rec.undo(txnum);
         }
      }
   }

   /**
    * Does a complete database recovery.
    * The method iterates through the simpledb.log records.
    * Whenever it finds a simpledb.log simpledb.record for an unfinished
    * transaction, it calls undo() on that simpledb.record.
    * The method stops when it encounters a CHECKPOINT simpledb.record
    * or the end of the simpledb.log.
    */
   private void doRecover() {
      Collection<Integer> finishedTxs = new ArrayList<Integer>();
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         if (rec.op() == CHECKPOINT)
            return;
         if (rec.op() == COMMIT || rec.op() == ROLLBACK)
            finishedTxs.add(rec.txNumber());
         else if (!finishedTxs.contains(rec.txNumber()))
            rec.undo(txnum);
      }
   }

   /**
    * Determines whether a block comes from a temporary simpledb.file or not.
    */
   private boolean isTempBlock(Block blk) {
      return blk.fileName().startsWith("temp");
   }
}
