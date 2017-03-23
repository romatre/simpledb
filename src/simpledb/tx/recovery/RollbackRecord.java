package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;

/**
 * The ROLLBACK simpledb.log simpledb.record.
 * @author Edward Sciore
 */
class RollbackRecord implements LogRecord {
   private int txnum;
   
   /**
    * Creates a new rollback simpledb.log simpledb.record for the specified transaction.
    * @param txnum the ID of the specified transaction
    */
   public RollbackRecord(int txnum) {
      this.txnum = txnum;
   }
   
   /**
    * Creates a simpledb.log simpledb.record by reading one other value from the simpledb.log.
    * @param rec the basic simpledb.log simpledb.record
    */
   public RollbackRecord(BasicLogRecord rec) {
      txnum = rec.nextInt();
   }
   
   /** 
    * Writes a rollback simpledb.record to the simpledb.log.
    * This simpledb.log simpledb.record contains the ROLLBACK operator,
    * followed by the transaction id.
    * @return the LSN of the last simpledb.log value
    */
   public int writeToLog() {
      Object[] rec = new Object[] {ROLLBACK, txnum};
      return logMgr.append(rec);
   }
   
   public int op() {
      return ROLLBACK;
   }
   
   public int txNumber() {
      return txnum;
   }
   
   /**
    * Does nothing, because a rollback simpledb.record
    * contains no undo information.
    */
   public void undo(int txnum) {}
   
   public String toString() {
      return "<ROLLBACK " + txnum + ">";
   }
}
