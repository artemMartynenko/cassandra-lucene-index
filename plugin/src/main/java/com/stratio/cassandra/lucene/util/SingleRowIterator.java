package com.stratio.cassandra.lucene.util;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class SingleRowIterator implements RowIterator {



    private final RowIterator iterator;
    private final Optional<Row> headRow;
    private final Optional<Function<Row, Row>> decorator;

    private final CFMetaData metaData;
    private final DecoratedKey partitionKey;
    private final PartitionColumns columns;
    private final Row staticRow;
    private final Iterator<Row> singleIterator;

    public final Row row;

    public SingleRowIterator(RowIterator iterator, Row headRow, Function<Row, Row> decorator) {


        this.iterator = iterator;
        this.headRow = Optional.ofNullable(headRow);
        this.decorator = Optional.ofNullable(decorator);

        this.row = this.headRow.orElse(iterator.next());

        this.metaData = iterator.metadata();
        this.partitionKey = iterator.partitionKey();
        this.columns = iterator.columns();
        this.staticRow = iterator.staticRow();
        this.singleIterator = Collections.singletonList(row).iterator();

    }

    public SingleRowIterator(RowIterator iterator) {
        this(iterator, null, null);
    }

    public SingleRowIterator decorate(Function<Row, Row> decorator){
        return new SingleRowIterator(iterator, row, decorator);
    }


    @Override
    public CFMetaData metadata() {
        return metaData;
    }

    @Override
    public boolean isReverseOrder() {
        return false;
    }

    @Override
    public PartitionColumns columns() {
        return columns;
    }

    @Override
    public DecoratedKey partitionKey() {
        return partitionKey;
    }

    @Override
    public Row staticRow() {
        return staticRow;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasNext() {
        return singleIterator.hasNext();
    }

    @Override
    public Row next() {
        Row row  = singleIterator.next();
        return decorator.map(rowRowFunction -> rowRowFunction.apply(row)).orElse(row);
    }
}
