package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;

/**
 * The COMMIT simpledb.log simpledb.record
 * @author Edward Sciore
 */
class CommitRecord implements LogRecord {
   private int txnum;
   
   /**
    * Creates a new commit simpledb.log simpledb.record for the specified transaction.
    * @param txnum the ID of the specified transaction
    */
   public CommitRecord(int txnum) {
      this.txnum = txnum;
   }
   
   /**
    * Creates a simpledb.log simpledb.record by reading one other value from the simpledb.log.
    * @param rec the basic simpledb.log simpledb.record
    */
   public CommitRecord(BasicLogRecord rec) {
      txnum = rec.nextInt();
   }
   
   /** 
    * Writes a commit simpledb.record to the simpledb.log.
    * This simpledb.log simpledb.record contains the COMMIT operator,
    * followed by the transaction id.
    * @return the LSN of the last simpledb.log value
    */
   public int writeToLog() {
      Object[] rec = new Object[] {COMMIT, txnum};
      return logMgr.append(rec);
   }
   
   public int op() {
      return COMMIT;
   }
   
   public int txNumber() {
      return txnum;
   }
   
   /**
    * Does nothing, because a commit simpledb.record
    * contains no undo information.
    */
   public void undo(int txnum) {}
   
   public String toString() {
      return "<COMMIT " + txnum + ">";
   }
}
