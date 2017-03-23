package simpledb.query;

import simpledb.record.Schema;

/**
 * The interface implemented by each simpledb.query plan.
 * There is a Plan class for each relational algebra operator.
 * @author Edward Sciore
 *
 */
public interface Plan {
   
   /**
    * Opens a scan corresponding to this plan.
    * The scan will be positioned before its first simpledb.record.
    * @return a scan
    */
   public Scan   open();
   
   /**
    * Returns an estimate of the number of block accesses
    * that will occur when the scan is read to completion.
    * @return the estimated number of block accesses
    */
   public int    blocksAccessed();
   
   /**
    * Returns an estimate of the number of records
    * in the simpledb.query's output table.
    * @return the estimated number of output records
    */
   public int    recordsOutput();
   
   /**
    * Returns an estimate of the number of distinct values
    * for the specified field in the simpledb.query's output table.
    * @param fldname the name of a field
    * @return the estimated number of distinct field values in the output
    */
   public int    distinctValues(String fldname);
   
   /**
    * Returns the schema of the simpledb.query.
    * @return the simpledb.query's schema
    */
   public Schema schema();
}
