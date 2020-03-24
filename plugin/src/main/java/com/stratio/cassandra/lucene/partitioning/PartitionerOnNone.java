package com.stratio.cassandra.lucene.partitioning;

import com.google.common.collect.Lists;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;

import java.util.List;

/**{@link Partitioner} with no action, equivalent to just don't partitioning the index.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class PartitionerOnNone implements Partitioner{

    private final int numPartitions;
    private final List<Integer> allPartitions;

    public PartitionerOnNone() {
        this.numPartitions = 1;
        this.allPartitions = Lists.newArrayList(0);
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public List<Integer> allPartitions() {
        return allPartitions;
    }

    @Override
    public int partition(DecoratedKey key) {
        return 0;
    }

    @Override
    public List<Integer> partitions(ReadCommand command) {
        return allPartitions;
    }


    /** {@link PartitionerOnNone} builder. */
    static class Builder implements Partitioner.Builder<PartitionerOnNone>{

        public Builder() {
        }
        @Override
        public PartitionerOnNone build(CFMetaData metaData) {
            return new PartitionerOnNone();
        }
    }
}
