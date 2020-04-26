package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.index.DocumentIterator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link UnfilteredPartitionIterator} for retrieving rows from a {@link DocumentIterator}.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 **/
public class IndexReaderExcludingDataCenter implements UnfilteredPartitionIterator {

    public static final Logger LOGGER = LoggerFactory.getLogger(IndexReaderExcludingDataCenter.class);

    protected final ReadCommand command;
    protected final ColumnFamilyStore table;

    public IndexReaderExcludingDataCenter(ReadCommand command,
                                          ColumnFamilyStore table) {
        this.command = command;
        this.table = table;
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

    }

    @Override
    public boolean hasNext() {
        LOGGER.warn("You are executing a query against a excluded datacenter node");
        return false;
    }

    @Override
    public UnfilteredRowIterator next() {
        LOGGER.warn("You are executing a query against a excluded datacenter node");
        return null;
    }
}
