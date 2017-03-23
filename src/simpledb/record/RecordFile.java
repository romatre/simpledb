package simpledb.record;

import simpledb.file.Block;
import simpledb.tx.Transaction;

/**
 * Manages a simpledb.file of records.
 * There are methods for iterating through the records
 * and accessing their contents.
 * @author Edward Sciore
 */
public class RecordFile {
   private TableInfo ti;
   private Transaction tx;
   private String filename;
   private RecordPage rp;
   private int currentblknum;
   
   /**
    * Constructs an object to manage a simpledb.file of records.
    * If the simpledb.file does not exist, it is created.
    * @param ti the table simpledb.metadata
    * @param tx the transaction
    */
   public RecordFile(TableInfo ti, Transaction tx) {
      this.ti = ti;
      this.tx = tx;
      filename = ti.fileName();
      if (tx.size(filename) == 0)
         appendBlock();
      moveTo(0);
   }
   
   /**
    * Closes the simpledb.record simpledb.file.
    */
   public void close() {
      rp.close();
   }
   
   /**
    * Positions the current simpledb.record so that a call to method next
    * will wind up at the first simpledb.record.
    */
   public void beforeFirst() {
      moveTo(0);
   }
   
   /**
    * Moves to the next simpledb.record. Returns false if there
    * is no next simpledb.record.
    * @return false if there is no next simpledb.record.
    */
   public boolean next() {
      while (true) {
         if (rp.next())
            return true;
         if (atLastBlock())
            return false;
         moveTo(currentblknum + 1);
      }
   }
   
   /**
    * Returns the value of the specified field
    * in the current simpledb.record.
    * @param fldname the name of the field
    * @return the integer value at that field
    */
   public int getInt(String fldname) {
      return rp.getInt(fldname);
   }
   
   /**
    * Returns the value of the specified field
    * in the current simpledb.record.
    * @param fldname the name of the field
    * @return the string value at that field
    */
   public String getString(String fldname) {
      return rp.getString(fldname);
   }
   
   /**
    * Sets the value of the specified field 
    * in the current simpledb.record.
    * @param fldname the name of the field
    * @param val the new value for the field
    */
   public void setInt(String fldname, int val) {
      rp.setInt(fldname, val);
   }
   
   /**
    * Sets the value of the specified field 
    * in the current simpledb.record.
    * @param fldname the name of the field
    * @param val the new value for the field
    */
   public void setString(String fldname, String val) {
      rp.setString(fldname, val);
   }
   
   /**
    * Deletes the current simpledb.record.
    * The client must call next() to move to
    * the next simpledb.record.
    * Calls to methods on a deleted simpledb.record
    * have unspecified behavior.
    */
   public void delete() {
      rp.delete();
   }
   
   /**
    * Inserts a new, blank simpledb.record somewhere in the simpledb.file
    * beginning at the current simpledb.record.
    * If the new simpledb.record does not fit into an existing block,
    * then a new block is appended to the simpledb.file.
    */
   public void insert() {
      while (!rp.insert()) {
         if (atLastBlock())
            appendBlock();
         moveTo(currentblknum + 1);
      }
   }
   
   /**
    * Positions the current simpledb.record as indicated by the
    * specified RID. 
    * @param rid a simpledb.record identifier
    */
   public void moveToRid(RID rid) {
      moveTo(rid.blockNumber());
      rp.moveToId(rid.id());
   }
   
   /**
    * Returns the RID of the current simpledb.record.
    * @return a simpledb.record identifier
    */
   public RID currentRid() {
      int id = rp.currentId();
      return new RID(currentblknum, id);
   }
   
   private void moveTo(int b) {
      if (rp != null)
         rp.close();
      currentblknum = b;
      Block blk = new Block(filename, currentblknum);
      rp = new RecordPage(blk, ti, tx);
   }
   
   private boolean atLastBlock() {
      return currentblknum == tx.size(filename) - 1;
   }
   
   private void appendBlock() {
      RecordFormatter fmtr = new RecordFormatter(ti);
      tx.append(filename, fmtr);
   }
}