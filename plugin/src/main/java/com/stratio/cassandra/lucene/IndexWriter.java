package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.util.Tracer;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link Index.Indexer} for Lucene-based index.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 **/
public abstract class IndexWriter implements Index.Indexer {

    private static final Tracer TRACER = new Tracer();
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexWriter.class);


    protected final  IndexService service;
    protected final DecoratedKey key;
    protected final int nowInSec;
    protected final OpOrder.Group opOrder;
    protected final IndexTransaction.Type transactionType;
    protected final CFMetaData metaData;
    protected final ColumnFamilyStore table;


    /**
     * @param service         the service to perform the indexing operation
     * @param key             key of the partition being modified
     * @param nowInSec        current time of the update operation
     * @param opOrder         operation group spanning the update operation
     * @param transactionType what kind of update is being performed on the base data
     * */
    public IndexWriter(IndexService service, DecoratedKey key, int nowInSec, OpOrder.Group opOrder, IndexTransaction.Type transactionType) {
        this.service = service;
        this.key = key;
        this.nowInSec = nowInSec;
        this.opOrder = opOrder;
        this.transactionType = transactionType;

        metaData = service.metaData;
        table = service.table;
    }

    @Override
    public void begin() {
        LOGGER.trace("Begin transaction {}", transactionType);
    }

    @Override
    public void partitionDelete(DeletionTime deletionTime) {
        LOGGER.trace("Delete partition during  {}: {}", transactionType, deletionTime);
        delete();
    }

    @Override
    public void rangeTombstone(RangeTombstone tombstone) {
        LOGGER.trace("Range tombstone during  {}: {}", transactionType, tombstone);
        delete(tombstone);
    }

    @Override
    public void insertRow(Row row) {
        LOGGER.trace("Insert row during {}: {}", transactionType, row);
        tryIndex(row);
    }

    @Override
    public void updateRow(Row oldRowData, Row newRowData) {
        LOGGER.trace("Update row during {}: {} TO {}", transactionType, oldRowData, newRowData);
        tryIndex(newRowData);
    }

    @Override
    public void removeRow(Row row) {
        LOGGER.trace("Remove row during {}: {}", transactionType, row);
        tryIndex(row);
    }

    @Override
    public void finish() {
        // Skip on cleanups
        if(!transactionType.equals(IndexTransaction.Type.CLEANUP)){
            // Finish with mutual exclusion on partition
            service.readBeforeWriteLocker.run(key, () -> {
                commit();
                return key;
            });
        }
    }


    /** Deletes all the partition. */
    protected abstract void delete();

    /** Deletes all the rows in the specified tombstone. */
    protected abstract void delete(RangeTombstone tombstone);


    /** Try indexing the row. If the row does not affect index it is not indexed */
    private void tryIndex(Row row){
        if(service.doesAffectIndex(row)){
            index(row);
        }else {
            TRACER.trace("Lucene index skipping row");
        }
    }

    /** Indexes the specified row. It behaviours as an upsert and may involve read-before-write.
     *
     * @param row the row to be indexed.
     */
    protected abstract void index(Row row);


    /** Retrieves from the local storage the rows satisfying the specified read command.
     *
     * @param command a single partition read command
     * @return a row iterator
     */
    protected RowIterator read(SinglePartitionReadCommand command){
        try (ReadExecutionController controller = command.executionController()) {
            UnfilteredRowIterator unfilteredRows = command.queryMemtableAndDisk(table, controller);
            return UnfilteredRowIterators.filter(unfilteredRows, nowInSec);
        }
    }

    /** Commits all pending writes */
    protected abstract void commit();

}
