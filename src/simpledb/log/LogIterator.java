package simpledb.log;

import static simpledb.file.Page.INT_SIZE;
import simpledb.file.*;
import java.util.Iterator;

/**
 * A class that provides the ability to move through the
 * records of the simpledb.log simpledb.file in reverse order.
 * 
 * @author Edward Sciore
 */
class LogIterator implements Iterator<BasicLogRecord> {
   private Block blk;
   private Page pg = new Page();
   private int currentrec;
   
   /**
    * Creates an iterator for the records in the simpledb.log simpledb.file,
    * positioned after the last simpledb.log simpledb.record.
    * This constructor is called exclusively by
    * {@link LogMgr#iterator()}.
    */
   LogIterator(Block blk) {
      this.blk = blk;
      pg.read(blk);
      currentrec = pg.getInt(LogMgr.LAST_POS);
   }
   
   /**
    * Determines if the current simpledb.log simpledb.record
    * is the earliest simpledb.record in the simpledb.log simpledb.file.
    * @return true if there is an earlier simpledb.record
    */
   public boolean hasNext() {
      return currentrec>0 || blk.number()>0;
   }
   
   /**
    * Moves to the next simpledb.log simpledb.record in reverse order.
    * If the current simpledb.log simpledb.record is the earliest in its block,
    * then the method moves to the next oldest block,
    * and returns the simpledb.log simpledb.record from there.
    * @return the next earliest simpledb.log simpledb.record
    */
   public BasicLogRecord next() {
      if (currentrec == 0) 
         moveToNextBlock();
      currentrec = pg.getInt(currentrec);
      return new BasicLogRecord(pg, currentrec+INT_SIZE);
   }
   
   public void remove() {
      throw new UnsupportedOperationException();
   }
   
   /**
    * Moves to the next simpledb.log block in reverse order,
    * and positions it after the last simpledb.record in that block.
    */
   private void moveToNextBlock() {
      blk = new Block(blk.fileName(), blk.number()-1);
      pg.read(blk);
      currentrec = pg.getInt(LogMgr.LAST_POS);
   }
}
