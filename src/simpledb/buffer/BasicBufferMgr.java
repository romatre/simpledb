package simpledb.buffer;

import simpledb.file.*;
import simpledb.server.SimpleDB;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private int latestPinned;

   /**
    * Creates a simpledb.buffer manager having the specified number
    * of simpledb.buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of simpledb.buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++)
         bufferpool[i] = new Buffer();
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a simpledb.buffer to the specified block.
    * If there is already a simpledb.buffer assigned to that block
    * then that simpledb.buffer is used;
    * otherwise, an unpinned simpledb.buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned simpledb.buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Allocates a new block in the specified simpledb.file, and
    * pins a simpledb.buffer to it.
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the simpledb.file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned simpledb.buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified simpledb.buffer.
    * @param buff the simpledb.buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
      for (Buffer buff : bufferpool) {
         Block b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;
   }
   
   private Buffer chooseUnpinnedBuffer() {
      switch (SimpleDB.STRATEGY) {
         case "naif":
            return chooseUnpinnedBufferNaif();
         case "clock":
            return chooseUnpinnedBufferClock();
         case "lru":
            return chooseUnpinnedBufferLRU();
      }
      return null;
   }

   private Buffer chooseUnpinnedBufferNaif() {
      for (Buffer buff : bufferpool)
         if (!buff.isPinned())
            return buff;
      return null;
   }

   private Buffer chooseUnpinnedBufferClock() {
      int length = bufferpool.length;
      for(int i = latestPinned; i < length; i++) {
         if (!bufferpool[i].isPinned()) {
            this.latestPinned = i;
            return bufferpool[i];
         }
      }
      for(int i = 0; i < latestPinned; i++) {
         if (!bufferpool[i].isPinned()) {
            this.latestPinned = i;
            return bufferpool[i];
         }
      }
      return null;
   }

   private Buffer chooseUnpinnedBufferLRU() {
      Buffer buffer = null;

      for (int i = 0; i < bufferpool.length; i++) {
         if (!bufferpool[i].isPinned()) {
            buffer = bufferpool[i];
            break;
         }
      }

      for(int i = 0; i < bufferpool.length; i++) {
         if(!bufferpool[i].isPinned() && bufferpool[i].getLatestUsage() < buffer.getLatestUsage()) {
            buffer = bufferpool[i];
         }
      }

      buffer.setLatestUsage(System.currentTimeMillis());

      return buffer;
   }

}
