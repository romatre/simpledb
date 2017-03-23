package simpledb.index;

import simpledb.record.RID;
import simpledb.query.Constant;

/**
 * This interface contains methods to traverse an simpledb.index.
 * @author Edward Sciore
 *
 */
public interface Index {
   
   /**
    * Positions the simpledb.index before the first simpledb.record
    * having the specified search key.
    * @param searchkey the search key value.
    */
   public void    beforeFirst(Constant searchkey);
   
   /**
    * Moves the simpledb.index to the next simpledb.record having the
    * search key specified in the beforeFirst method. 
    * Returns false if there are no more such simpledb.index records.
    * @return false if no other simpledb.index records have the search key.
    */
   public boolean next();
   
   /**
    * Returns the dataRID value stored in the current simpledb.index simpledb.record.
    * @return the dataRID stored in the current simpledb.index simpledb.record.
    */
   public RID     getDataRid();
   
   /**
    * Inserts an simpledb.index simpledb.record having the specified
    * dataval and dataRID values.
    * @param dataval the dataval in the new simpledb.index simpledb.record.
    * @param datarid the dataRID in the new simpledb.index simpledb.record.
    */
   public void    insert(Constant dataval, RID datarid);
   
   /**
    * Deletes the simpledb.index simpledb.record having the specified
    * dataval and dataRID values.
    * @param dataval the dataval of the deleted simpledb.index simpledb.record
    * @param datarid the dataRID of the deleted simpledb.index simpledb.record
    */
   public void    delete(Constant dataval, RID datarid);
   
   /**
    * Closes the simpledb.index.
    */
   public void    close();
}
