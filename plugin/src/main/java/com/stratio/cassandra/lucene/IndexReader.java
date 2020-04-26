package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.index.DocumentIterator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.CloseableIterator;

import java.util.Optional;

/** {@link UnfilteredPartitionIterator} for retrieving rows from a {@link DocumentIterator}.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 *
 **/
public abstract class IndexReader implements UnfilteredPartitionIterator {

    protected final ReadCommand command;
    protected final ColumnFamilyStore table;
    protected final ReadExecutionController controller;
    protected final DocumentIterator documents;

    protected Optional<UnfilteredRowIterator> nextData = Optional.empty();


    /**
     * @param command    the read command
     * @param table      the base table
     * @param controller the read execution controller
     * @param documents  the documents iterator
     * */
    public IndexReader(ReadCommand command, ColumnFamilyStore table, ReadExecutionController controller, DocumentIterator documents) {
        this.command = command;
        this.table = table;
        this.controller = controller;
        this.documents = documents;
    }

    @Override
    public boolean isForThrift() {
        return command.isForThrift();
    }

    @Override
    public CFMetaData metadata() {
        return table.metadata;
    }

    @Override
    public void close() {
        try {
            nextData.ifPresent(CloseableIterator::close);
        }finally {
            try {
                documents.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean hasNext() {
        return prepareNext();
    }

    @Override
    public UnfilteredRowIterator next() {
        if(!nextData.isPresent()) prepareNext();
        UnfilteredRowIterator result = nextData.orElse(null);
        nextData = Optional.empty();
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    protected abstract boolean prepareNext();


    protected UnfilteredRowIterator read(DecoratedKey key, ClusteringIndexFilter filter){
        return SinglePartitionReadCommand.create(
                isForThrift(),
                table.metadata,
                command.nowInSec(),
                command.columnFilter(),
                command.rowFilter(),
                command.limits(),
                key,
                filter).queryMemtableAndDisk(table, controller);
    }

}
