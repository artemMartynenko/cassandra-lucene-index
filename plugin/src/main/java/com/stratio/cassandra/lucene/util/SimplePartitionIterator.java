package com.stratio.cassandra.lucene.util;

import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class SimplePartitionIterator implements PartitionIterator {


    private final Collection<SingleRowIterator> rows;
    private final Iterator<SingleRowIterator> iterator;

    public SimplePartitionIterator(Collection<SingleRowIterator> rows) {
        this.rows = rows;
        this.iterator = rows.iterator();
    }

    @Override
    public void close() {
        iterator.forEachRemaining(SingleRowIterator::close);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public RowIterator next() {
        return iterator.next();
    }
}
