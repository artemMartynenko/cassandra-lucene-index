package com.stratio.cassandra.lucene.partitioning;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.Token;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**{@link Partitioner} based on the partition key token. Rows will be stored in an index partition
 * determined by the hash of the partition key token. Partition-directed searches will be routed to
 * a single partition, increasing performance. However, token range searches will be routed to all
 * the partitions, with a slightly lower performance.
 *
 * This partitioner guarantees an excellent load balancing between index partitions.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class PartitionerOnToken implements Partitioner {


    private final int partitions;
    private final List<Integer> allPartitions;

    /**
     * @param partitions the number of index partitions per node
     * */
    public PartitionerOnToken(int partitions) {
        if (partitions <= 0)
            throw new IndexException("The number of partitions should be strictly positive but found "+partitions);

        this.partitions = partitions;
        this.allPartitions = IntStream.range(0, partitions).boxed().collect(Collectors.toList());
    }


    private int partition(Token token){
        return (int) (Math.abs((long)token.getTokenValue()) % partitions);
    }

    @Override
    public int numPartitions() {
        return partitions;
    }

    @Override
    public List<Integer> allPartitions() {
        return allPartitions;
    }

    @Override
    public int partition(DecoratedKey key) {
        return partition(key.getToken());
    }

    @Override
    public List<Integer> partitions(ReadCommand command) {
        if(command instanceof SinglePartitionReadCommand){
            DecoratedKey partitionKey = ((SinglePartitionReadCommand) command).partitionKey();
            return Lists.newArrayList(partition(partitionKey));
        } else if(command instanceof PartitionRangeReadCommand){
            DataRange range = ((PartitionRangeReadCommand) command).dataRange();
            Token start = range.startKey().getToken();
            Token stop = range.stopKey().getToken();
            return start.equals(stop) && !start.isMinimum() ? Lists.newArrayList(partition(start)) : allPartitions;
        }else {
            throw new IndexException("Unsupported read command type: "+command.getClass());
        }
    }

    /**{@link PartitionerOnToken} builder.*/
    static class Builder implements Partitioner.Builder<PartitionerOnToken>{

        private final int partitions;

        /**
         * @param partitions the number of index partitions per node
         */
        Builder(@JsonProperty("partitions") int partitions) {
            this.partitions = partitions;
        }


        @Override
        public PartitionerOnToken build(CFMetaData metaData) {
            return new PartitionerOnToken(partitions);
        }
    }
}
