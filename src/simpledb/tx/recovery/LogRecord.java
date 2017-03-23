package simpledb.tx.recovery;

import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

/**
 * The interface implemented by each type of simpledb.log simpledb.record.
 * @author Edward Sciore
 */
public interface LogRecord {
   /**
    * The six different types of simpledb.log simpledb.record
    */
   static final int CHECKPOINT = 0, START = 1,
      COMMIT = 2, ROLLBACK  = 3,
      SETINT = 4, SETSTRING = 5;
   
   static final LogMgr logMgr = SimpleDB.logMgr();
   
   /**
    * Writes the simpledb.record to the simpledb.log and returns its LSN.
    * @return the LSN of the simpledb.record in the simpledb.log
    */
   int writeToLog();
   
   /**
    * Returns the simpledb.log simpledb.record's type.
    * @return the simpledb.log simpledb.record's type
    */
   int op();
   
   /**
    * Returns the transaction id stored with
    * the simpledb.log simpledb.record.
    * @return the simpledb.log simpledb.record's transaction id
    */
   int txNumber();
   
   /**
    * Undoes the operation encoded by this simpledb.log simpledb.record.
    * The only simpledb.log simpledb.record types for which this method
    * does anything interesting are SETINT and SETSTRING.
    * @param txnum the id of the transaction that is performing the undo.
    */
   void undo(int txnum);
}