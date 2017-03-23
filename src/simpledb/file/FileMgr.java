package simpledb.file;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import simpledb.server.SimpleDB;
import simpledb.stats.BasicFileStats;

import static simpledb.server.SimpleDB.BLOCK_SIZE;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * The SimpleDB simpledb.file manager.
 * The database system stores its data as files within a specified directory.
 * The simpledb.file manager provides methods for reading the contents of
 * a simpledb.file block to a Java byte simpledb.buffer,
 * writing the contents of a byte simpledb.buffer to a simpledb.file block,
 * and appending the contents of a byte simpledb.buffer to the end of a simpledb.file.
 * These methods are called exclusively by the class {@link simpledb.file.Page Page},
 * and are thus package-private.
 * The class also contains two public methods:
 * Method {@link #isNew() isNew} is called during system initialization by {@link simpledb.server.SimpleDB#init}.
 * Method {@link #size(String) size} is called by the simpledb.log manager and transaction manager to
 * determine the end of the simpledb.file.
 * @author Edward Sciore
 */
public class FileMgr {
   private File dbDirectory;
   private boolean isNew;
   private Map<String,FileChannel> openFiles = new HashMap<String,FileChannel>();
   private Map<String, BasicFileStats> blockStatsFile = new HashMap<String, BasicFileStats>();

   private void updateReadBlockStats(Block blk, ByteBuffer bb) {
      this.getMapStats().get(blk.fileName()).incrementBlockRead();
   }

   private void updateWriteBlockStats(Block blk, ByteBuffer bb) {
      this.getMapStats().get(blk.fileName()).incrementBlockWritten();
   }

   public final Map<String,BasicFileStats> getMapStats() {
      return this.blockStatsFile;
   }

   public static JSONObject getBlockStat(String fileName, BasicFileStats fileStats) throws JSONException {
      JSONObject blockStat = new JSONObject();
      blockStat.put("fileName", fileName);
      blockStat.put("readBlocks", fileStats.getBlockRead());
      blockStat.put("writtenBlocks", fileStats.getBlockWritten());
      return blockStat;
   }

   public static JSONArray getAllBlockStats() {
      JSONArray blockStats = new JSONArray();
      SimpleDB.fileMgr()
         .getMapStats()
         .forEach((fileName, fileStats) -> {
            try {
               blockStats.put(getBlockStat(fileName, fileStats));
            } catch (JSONException e) {
            e.printStackTrace();
            }
         });
      return blockStats;
   }

   public final void resetMapStats() {
      this.blockStatsFile = new HashMap<String, BasicFileStats>();
   }

   /**
    * Creates a simpledb.file manager for the specified database.
    * The database will be stored in a folder of that name
    * in the user's home directory.
    * If the folder does not exist, then a folder containing
    * an empty database is created automatically.
    * Files for all temporary tables (i.e. tables beginning with "temp") are deleted.
    * @param dbname the name of the directory that holds the database
    */
   public FileMgr(String dbname) {
      String homedir = System.getProperty("user.home");
      dbDirectory = new File(homedir, dbname);
      isNew = !dbDirectory.exists();

      // create the directory if the database is new
      if (isNew && !dbDirectory.mkdir())
         throw new RuntimeException("cannot create " + dbname);

      // remove any leftover temporary tables
      for (String filename : dbDirectory.list())
         if (filename.startsWith("temp"))
         new File(dbDirectory, filename).delete();
   }

   /**
    * Reads the contents of a disk block into a bytebuffer.
    * @param blk a reference to a disk block
    * @param bb  the bytebuffer
    */
   synchronized void read(Block blk, ByteBuffer bb) {
      try {
         bb.clear();
         FileChannel fc = getFile(blk.fileName());
         this.updateReadBlockStats(blk, bb);
         fc.read(bb, blk.number() * BLOCK_SIZE);
      }
      catch (IOException e) {
         throw new RuntimeException("cannot read block " + blk);
      }
   }

   /**
    * Writes the contents of a bytebuffer into a disk block.
    * @param blk a reference to a disk block
    * @param bb  the bytebuffer
    */
   synchronized void write(Block blk, ByteBuffer bb) {
      try {
         bb.rewind();
         FileChannel fc = getFile(blk.fileName());
         this.updateWriteBlockStats(blk, bb);
         fc.write(bb, blk.number() * BLOCK_SIZE);
      }
      catch (IOException e) {
         throw new RuntimeException("cannot write block" + blk);
      }
   }

   /**
    * Appends the contents of a bytebuffer to the end
    * of the specified simpledb.file.
    * @param filename the name of the simpledb.file
    * @param bb  the bytebuffer
    * @return a reference to the newly-created block.
    */
   synchronized Block append(String filename, ByteBuffer bb) {
      int newblknum = size(filename);
      Block blk = new Block(filename, newblknum);
      write(blk, bb);
      return blk;
   }

   /**
    * Returns the number of blocks in the specified simpledb.file.
    * @param filename the name of the simpledb.file
    * @return the number of blocks in the simpledb.file
    */
   public synchronized int size(String filename) {
      try {
         FileChannel fc = getFile(filename);
         return (int)(fc.size() / BLOCK_SIZE);
      }
      catch (IOException e) {
         throw new RuntimeException("cannot access " + filename);
      }
   }

   /**
    * Returns a boolean indicating whether the simpledb.file manager
    * had to create a new database directory.
    * @return true if the database is new
    */
   public boolean isNew() {
      return isNew;
   }

   /**
    * Returns the simpledb.file channel for the specified filename.
    * The simpledb.file channel is stored in a map keyed on the filename.
    * If the simpledb.file is not open, then it is opened and the simpledb.file channel
    * is added to the map.
    * @param filename the specified filename
    * @return the simpledb.file channel associated with the open simpledb.file.
    * @throws IOException
    */
   private FileChannel getFile(String filename) throws IOException {
      FileChannel fc = openFiles.get(filename);
      if (!this.blockStatsFile.containsKey(filename)) {
         this.blockStatsFile.put(filename, new BasicFileStats());
      }
      if (fc == null) {
         File dbTable = new File(dbDirectory, filename);
         RandomAccessFile f = new RandomAccessFile(dbTable, "rws");
         fc = f.getChannel();
         openFiles.put(filename, fc);
      }
      return fc;
   }
}
