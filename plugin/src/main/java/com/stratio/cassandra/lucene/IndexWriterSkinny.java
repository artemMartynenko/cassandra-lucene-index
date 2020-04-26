package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.util.Tracer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** {@link IndexWriter} for skinny rows.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 **/
public class IndexWriterSkinny extends IndexWriter{

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexWriterSkinny.class);
    private static final Tracer TRACER = new Tracer();


    /**
     * @param service         the service to perform the indexing operation
     * @param key             key of the partition being modified
     * @param nowInSec        current time of the update operation
     * @param opOrder         operation group spanning the update operation
     * @param transactionType what kind of update is being performed on the base data
     */
    private Optional<Row> row = Optional.empty();

    public IndexWriterSkinny(IndexService service, DecoratedKey key, int nowInSec, OpOrder.Group opOrder, IndexTransaction.Type transactionType) {
        super(service, key, nowInSec, opOrder, transactionType);
    }

    @Override
    protected void delete() {
        service.delete(key);
        row = Optional.empty();
    }

    @Override
    protected void delete(RangeTombstone tombstone) {
        LOGGER.warn("Ignoring range tombstone {} in skinny table", tombstone);
    }

    @Override
    protected void index(Row row) {
        this.row = Optional.of(row);
    }

    @Override
    protected void commit() {
        row.map(targetRow -> {
            if(transactionType.equals(IndexTransaction.Type.COMPACTION) || service.needsReadBeforeWrite(key, targetRow)){
                TRACER.trace("Lucene index reading before write");
                SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(metaData, nowInSec, key);
                RowIterator readRows = read(command);
                return readRows.hasNext() ? readRows.next() : targetRow;
            } else {
                return targetRow;
            }
        }).ifPresent(targetRow -> {
            if(targetRow.hasLiveData(nowInSec, metaData.enforceStrictLiveness())){
                TRACER.trace("Lucene index writing document");
                service.upsert(key, targetRow, nowInSec);
            } else {
                TRACER.trace("Lucene index deleting document");
                service.delete(key);
            }
        });
    }

}
