package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.util.Tracer;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.utils.concurrent.OpOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

/** {@link IndexWriter} for wide rows.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 **/
public class IndexWriterWide extends IndexWriter{

    private static final Tracer TRACER = new Tracer();

    /** The clustering keys of the rows needing read before write. */
    private final TreeSet<Clustering> clusterings;

    /** The rows ready to be written. */
    private final TreeMap<Clustering, Row> rows;



    /**
     * @param service         the service to perform the indexing operation
     * @param key             key of the partition being modified
     * @param nowInSec        current time of the update operation
     * @param opOrder         operation group spanning the update operation
     * @param transactionType what kind of update is being performed on the base data
     */
    public IndexWriterWide(IndexServiceWide service, DecoratedKey key, int nowInSec, OpOrder.Group opOrder, IndexTransaction.Type transactionType) {
        super(service, key, nowInSec, opOrder, transactionType);
        clusterings = new TreeSet<>(metaData.comparator);
        rows = new TreeMap<>(metaData.comparator);
    }

    @Override
    protected void delete() {
        service.delete(key);
        clusterings.clear();
        rows.clear();
    }

    @Override
    protected void delete(RangeTombstone tombstone) {
        Slice slice = tombstone.deletedSlice();
        ((IndexServiceWide)service).delete(key, slice);
        clusterings.removeIf(clustering -> slice.includes(metaData.comparator, clustering));
        rows.keySet().removeIf(clustering -> slice.includes(metaData.comparator, clustering));
    }

    @Override
    protected void index(Row row) {
        Clustering clustering = row.clustering();
        if(service.needsReadBeforeWrite(key, row)){
            TRACER.trace("Lucene index doing read before write");
            clusterings.add(clustering);
        } else {
            TRACER.trace("Lucene index skipping read before write");
            rows.put(clustering, row);
        }
    }

    @Override
    protected void commit() {


        List<Clustering> rowsToDelete = new ArrayList<>();

        // Read required rows from storage engine
        if(!clusterings.isEmpty()){
            SinglePartitionReadCommand command = SinglePartitionReadCommand.create(metaData, nowInSec, key, clusterings);
            read(command).forEachRemaining(row -> {
                rows.put(row.clustering(), row);
            });
            clusterings.iterator().forEachRemaining(clustering -> {
                if(rows.get(clustering) == null){
                 rowsToDelete.add(clustering);
                }
            });
        }

        // Write rows
        rows.forEach((clustering, row) -> {
            if(row.hasLiveData(nowInSec, metaData.enforceStrictLiveness())){
                TRACER.trace("Lucene index writing document");
                service.upsert(key, row, nowInSec);
            }else {
                TRACER.trace("Lucene index deleting document");
                service.delete(key, clustering);
            }
        });

        rowsToDelete.forEach(clustering -> {
            TRACER.trace("Lucene index deleting document");
            service.delete(key, clustering);
        });
    }
}
