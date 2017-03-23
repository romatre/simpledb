package simpledb.buffer;

import simpledb.server.SimpleDB;
import simpledb.file.*;

/**
 * An individual simpledb.buffer.
 * A simpledb.buffer wraps a page and stores information about its status,
 * such as the disk block associated with the page,
 * the number of times the block has been pinned,
 * whether the contents of the page have been modified,
 * and if so, the id of the modifying transaction and
 * the LSN of the corresponding simpledb.log simpledb.record.
 * @author Edward Sciore
 */
public class Buffer {
   private Page contents = new Page();
   private Block blk = null;
   private int pins = 0;
   private int modifiedBy = -1;  // negative means not modified
   private int logSequenceNumber = -1; // negative means no corresponding simpledb.log simpledb.record

   /**
    * Creates a new simpledb.buffer, wrapping a new
    * {@link simpledb.file.Page page}.  
    * This constructor is called exclusively by the 
    * class {@link BasicBufferMgr}.   
    * It depends on  the 
    * {@link simpledb.log.LogMgr LogMgr} object 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * That object is created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    */
   public Buffer() {}
   
   /**
    * Returns the integer value at the specified offset of the
    * simpledb.buffer's page.
    * If an integer was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the integer value at that offset
    */
   public int getInt(int offset) {
      return contents.getInt(offset);
   }

   /**
    * Returns the string value at the specified offset of the
    * simpledb.buffer's page.
    * If a string was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the string value at that offset
    */
   public String getString(int offset) {
      return contents.getString(offset);
   }

   /**
    * Writes an integer to the specified offset of the
    * simpledb.buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate simpledb.log simpledb.record.
    * The simpledb.buffer saves the id of the transaction
    * and the LSN of the simpledb.log simpledb.record.
    * A negative lsn value indicates that a simpledb.log simpledb.record
    * was not necessary.
    * @param offset the byte offset within the page
    * @param val the new integer value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding simpledb.log simpledb.record
    */
   public void setInt(int offset, int val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setInt(offset, val);
   }

   /**
    * Writes a string to the specified offset of the
    * simpledb.buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate simpledb.log simpledb.record.
    * A negative lsn value indicates that a simpledb.log simpledb.record
    * was not necessary.
    * The simpledb.buffer saves the id of the transaction
    * and the LSN of the simpledb.log simpledb.record.
    * @param offset the byte offset within the page
    * @param val the new string value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding simpledb.log simpledb.record
    */
   public void setString(int offset, String val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setString(offset, val);
   }

   /**
    * Returns a reference to the disk block
    * that the simpledb.buffer is pinned to.
    * @return a reference to a disk block
    */
   public Block block() {
      return blk;
   }

   /**
    * Writes the page to its disk block if the
    * page is dirty.
    * The method ensures that the corresponding simpledb.log
    * simpledb.record has been written to disk prior to writing
    * the page to disk.
    */
   void flush() {
      if (modifiedBy >= 0) {
         SimpleDB.logMgr().flush(logSequenceNumber);
         contents.write(blk);
         modifiedBy = -1;
      }
   }

   /**
    * Increases the simpledb.buffer's pin count.
    */
   void pin() {
      pins++;
   }

   /**
    * Decreases the simpledb.buffer's pin count.
    */
   void unpin() {
      pins--;
   }

   /**
    * Returns true if the simpledb.buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the simpledb.buffer is pinned
    */
   boolean isPinned() {
      return pins > 0;
   }

   /**
    * Returns true if the simpledb.buffer is dirty
    * due to a modification by the specified transaction.
    * @param txnum the id of the transaction
    * @return true if the transaction modified the simpledb.buffer
    */
   boolean isModifiedBy(int txnum) {
      return txnum == modifiedBy;
   }

   /**
    * Reads the contents of the specified block into
    * the simpledb.buffer's page.
    * If the simpledb.buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param b a reference to the data block
    */
   void assignToBlock(Block b) {
      flush();
      blk = b;
      contents.read(blk);
      pins = 0;
   }

   /**
    * Initializes the simpledb.buffer's page according to the specified formatter,
    * and appends the page to the specified simpledb.file.
    * If the simpledb.buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param filename the name of the simpledb.file
    * @param fmtr a page formatter, used to initialize the page
    */
   void assignToNew(String filename, PageFormatter fmtr) {
      flush();
      fmtr.format(contents);
      blk = contents.append(filename);
      pins = 0;
   }
}