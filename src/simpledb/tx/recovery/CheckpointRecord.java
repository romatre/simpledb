package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;

/**
 * The CHECKPOINT simpledb.log simpledb.record.
 * @author Edward Sciore
 */
class CheckpointRecord implements LogRecord {
   
   /**
    * Creates a quiescent checkpoint simpledb.record.
    */
   public CheckpointRecord() {}
   
   /**
    * Creates a simpledb.log simpledb.record by reading no other values
    * from the basic simpledb.log simpledb.record.
    * @param rec the basic simpledb.log simpledb.record
    */
   public CheckpointRecord(BasicLogRecord rec) {}
   
   /** 
    * Writes a checkpoint simpledb.record to the simpledb.log.
    * This simpledb.log simpledb.record contains the CHECKPOINT operator,
    * and nothing else.
    * @return the LSN of the last simpledb.log value
    */
   public int writeToLog() {
      Object[] rec = new Object[] {CHECKPOINT};
      return logMgr.append(rec);
   }
   
   public int op() {
      return CHECKPOINT;
   }
   
   /**
    * Checkpoint records have no associated transaction,
    * and so the method returns a "dummy", negative txid.
    */
   public int txNumber() {
      return -1; // dummy value
   }
   
   /**
    * Does nothing, because a checkpoint simpledb.record
    * contains no undo information.
    */
   public void undo(int txnum) {}
   
   public String toString() {
      return "<CHECKPOINT>";
   }
}
