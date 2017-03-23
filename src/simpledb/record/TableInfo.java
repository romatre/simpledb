package simpledb.record;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import java.util.*;

/**
 * The simpledb.metadata about a table and its records.
 * @author Edward Sciore
 */
public class TableInfo {
   private Schema schema;
   private Map<String,Integer> offsets;
   private int recordlen;
   private String tblname;
   
   /**
    * Creates a TableInfo object, given a table name
    * and schema. The constructor calculates the
    * physical offset of each field.
    * This constructor is used when a table is created. 
    * @param tblname the name of the table
    * @param schema the schema of the table's records
    */
   public TableInfo(String tblname, Schema schema) {
      this.schema = schema;
      this.tblname = tblname;
      offsets  = new HashMap<String,Integer>();
      int pos = 0;
      for (String fldname : schema.fields()) {
         offsets.put(fldname, pos);
         pos += lengthInBytes(fldname);
      }
      recordlen = pos;
   }
   
   /**
    * Creates a TableInfo object from the 
    * specified simpledb.metadata.
    * This constructor is used when the simpledb.metadata
    * is retrieved from the catalog.
    * @param tblname the name of the table
    * @param schema the schema of the table's records
    * @param offsets the already-calculated offsets of the fields within a simpledb.record
    * @param recordlen the already-calculated length of each simpledb.record
    */
   public TableInfo(String tblname, Schema schema, Map<String,Integer> offsets, int recordlen) {
      this.tblname   = tblname;
      this.schema    = schema;
      this.offsets   = offsets;
      this.recordlen = recordlen;
   }
   
   /**
    * Returns the filename assigned to this table.
    * Currently, the filename is the table name
    * followed by ".tbl".
    * @return the name of the simpledb.file assigned to the table
    */
   public String fileName() {
      return tblname + ".tbl";
   }
   
   /**
    * Returns the schema of the table's records
    * @return the table's simpledb.record schema
    */
   public Schema schema() {
      return schema;
   }
   
   /**
    * Returns the offset of a specified field within a simpledb.record
    * @param fldname the name of the field
    * @return the offset of that field within a simpledb.record
    */
   public int offset(String fldname) {
      return offsets.get(fldname);
   }
   
   /**
    * Returns the length of a simpledb.record, in bytes.
    * @return the length in bytes of a simpledb.record
    */
   public int recordLength() {
      return recordlen;
   }
   
   private int lengthInBytes(String fldname) {
      int fldtype = schema.type(fldname);
      if (fldtype == INTEGER)
         return INT_SIZE;
      else
         return STR_SIZE(schema.length(fldname));
   }
}