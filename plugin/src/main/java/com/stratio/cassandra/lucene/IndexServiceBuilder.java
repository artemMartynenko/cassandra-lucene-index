package com.stratio.cassandra.lucene;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.IndexMetadata;

/**
 * @author Artem Martynenko artem7mag@gmail.com
 **/
public class IndexServiceBuilder {


    /** Returns a new index service for the specified indexed table and index metadata.
     *
     * @param table         the indexed table
     * @param indexMetadata the index metadata
     * @return the index service
     */
    public static IndexService build(ColumnFamilyStore table, IndexMetadata indexMetadata){
        if(table.getComparator().subtypes().isEmpty()){
            return new IndexServiceSkinny(table, indexMetadata);
        }else {
            return new IndexServiceWide(table, indexMetadata);
        }
    }

}
