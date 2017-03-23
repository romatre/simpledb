package simpledb.planner;

import simpledb.tx.Transaction;
import simpledb.query.Plan;
import simpledb.parse.QueryData;

/**
 * The interface implemented by planners for 
 * the SQL select statement.
 * @author Edward Sciore
 *
 */
public interface QueryPlanner {
   
   /**
    * Creates a plan for the parsed simpledb.query.
    * @param data the parsed representation of the simpledb.query
    * @param tx the calling transaction
    * @return a plan for that simpledb.query
    */
   public Plan createPlan(QueryData data, Transaction tx);
}
