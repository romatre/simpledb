package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;

class StartRecord implements LogRecord {
   private int txnum;
   
   /**
    * Creates a new start simpledb.log simpledb.record for the specified transaction.
    * @param txnum the ID of the specified transaction
    */
   public StartRecord(int txnum) {
      this.txnum = txnum;
   }
   
   /**
    * Creates a simpledb.log simpledb.record by reading one other value from the simpledb.log.
    * @param rec the basic simpledb.log simpledb.record
    */
   public StartRecord(BasicLogRecord rec) {
      txnum = rec.nextInt();
   }
   
   /** 
    * Writes a start simpledb.record to the simpledb.log.
    * This simpledb.log simpledb.record contains the START operator,
    * followed by the transaction id.
    * @return the LSN of the last simpledb.log value
    */
   public int writeToLog() {
      Object[] rec = new Object[] {START, txnum};
      return logMgr.append(rec);
   }
   
   public int op() {
      return START;
   }
   
   public int txNumber() {
      return txnum;
   }
   
   /**
    * Does nothing, because a start simpledb.record
    * contains no undo information.
    */
   public void undo(int txnum) {}
   
   public String toString() {
      return "<START " + txnum + ">";
   }
}
