package simpledb.parse;

/**
 * The parser for the <i>create simpledb.index</i> statement.
 * @author Edward Sciore
 */
public class CreateIndexData {
   private String idxname, tblname, fldname;
   
   /**
    * Saves the table and field names of the specified simpledb.index.
    */
   public CreateIndexData(String idxname, String tblname, String fldname) {
      this.idxname = idxname;
      this.tblname = tblname;
      this.fldname = fldname;
   }
   
   /**
    * Returns the name of the simpledb.index.
    * @return the name of the simpledb.index
    */
   public String indexName() {
      return idxname;
   }
   
   /**
    * Returns the name of the indexed table.
    * @return the name of the indexed table
    */
   public String tableName() {
      return tblname;
   }
   
   /**
    * Returns the name of the indexed field.
    * @return the name of the indexed field
    */
   public String fieldName() {
      return fldname;
   }
}

