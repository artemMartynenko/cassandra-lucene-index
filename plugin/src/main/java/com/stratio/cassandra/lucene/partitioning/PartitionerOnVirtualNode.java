package com.stratio.cassandra.lucene.partitioning;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** {@link Partitioner} based on the partition key token. Rows will be stored in an index partition
 * determined by the virtual nodes token range. Partition-directed searches will be routed to a
 * single partition, increasing performance. However, unbounded token range searches will be routed
 * to all the partitions, with a slightly lower performance. Virtual node token range queries will
 * be routed to only one partition which increase performance in spark queries with virtual nodes rather
 * than partitioning on token.
 *
 * This partitioner load balance depends on virtual node token ranges assignation. The more virtual
 * nodes, the better distribution (more similarity in number of tokens that falls inside any virtual
 * node) between virtual nodes, the better load balance with this partitioner.
 *
 *
 * @author Eduardo Alonso `eduardoalonso@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class PartitionerOnVirtualNode implements Partitioner{

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionerOnVirtualNode.class);

    private final int vnodes_per_partition;
    private final List<Token> tokens;

    private int numTokens;
    private Map<Bounds<Token>, Integer> partitionPerBound;
    private int partition;
    private int numPartitions;
    private List<Integer> allPartitions;


    /**
     * @param vnodes_per_partition the number of virtual nodes that falls inside an index partition
     * */
    public PartitionerOnVirtualNode(int vnodes_per_partition, List<Token> tokens) {
        if (vnodes_per_partition <= 0) throw new IndexException(
                "The number of virtual nodes per partition should be strictly positive but found "+ vnodes_per_partition);

        if(tokens.size() == 1 )
            LOGGER.warn("You are using a PartitionerOnVirtualNode but cassandra is only configured with one token (not using virtual nodes.)");

        this.vnodes_per_partition = vnodes_per_partition;
        this.tokens = tokens;
        this.numTokens = tokens.size();
        this.numPartitions = calculateNumPartitions();
        this.allPartitions = IntStream.range(0, numPartitions).boxed().collect(Collectors.toList());
        this.partition = calculatePartition();
        this.partitionPerBound = calculatePartitionPerBound();
    }

    private int calculateNumPartitions(){
        return (int) Math.ceil((double) numTokens / (double) vnodes_per_partition);
    }

    private int calculatePartition(){
        return (int) Math.floor(((double) numPartitions - 1) / (double) vnodes_per_partition);
    }

    private Map<Bounds<Token>, Integer> calculatePartitionPerBound(){
        Map<Bounds<Token>, Integer> partitionPerBound = new HashMap<>();

        for (int i = 0; i < numPartitions -1 ; i++) {
            Token left = tokens.get(i);

            long rightToken = ((long)tokens.get(i + 1).getTokenValue()) - 1;
            Token right = new Murmur3Partitioner.LongToken(rightToken);

            Bounds<Token> bound = new Bounds<>(left, right);

            int partition = (int) Math.floor( (double)i / (double) vnodes_per_partition);

            partitionPerBound.put(bound, partition);
        }

        partitionPerBound.put(new Bounds<>(tokens.get(numPartitions -1), new Murmur3Partitioner.LongToken(Long.MAX_VALUE)), partition);

        if((long)tokens.get(0).getTokenValue() != Long.MIN_VALUE){
            partitionPerBound.put(new Bounds<>(new Murmur3Partitioner.LongToken(Long.MIN_VALUE), tokens.get(0)), partition);
        }

        return partitionPerBound;
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
        return partition(key.getToken());
    }

    @Override
    public List<Integer> partitions(ReadCommand command) {
        if(command instanceof SinglePartitionReadCommand){
            DecoratedKey partitionKey = ((SinglePartitionReadCommand) command).partitionKey();
            return Lists.newArrayList(partition(partitionKey));
        }else if(command instanceof PartitionRangeReadCommand){
            DataRange range = ((PartitionRangeReadCommand) command).dataRange();
            return partitions(range.startKey().getToken(), range.stopKey().getToken());
        }else {
            throw new IndexException("Unsupported read command type: "+command.getClass());
        }
    }

    /** Returns a list of the partitions involved in the range.
     *
     * @param lower the lower bound partition
     * @param upper the upper bound partition
     * @return a list of partitions involved in the range
     */
    public List<Integer> partitions(Token lower, Token upper){
     if(lower.equals(upper)){
         return  lower.isMinimum() ? allPartitions : Lists.newArrayList(partition(lower));
     } else {
         int lowerPartition = partition(lower);
         int upperPartition = partition(upper);

         if(lowerPartition <= upperPartition){
            return IntStream.range(lowerPartition, upperPartition).boxed().collect(Collectors.toList());
         } else {
             List<Integer> lowerRange = IntStream.range(lowerPartition, numTokens).boxed().collect(Collectors.toList());
             List<Integer> upperRange = IntStream.range(0, upperPartition).boxed().collect(Collectors.toList());
             lowerRange.addAll(upperRange);
             return lowerRange;
         }
     }
    }


    private int partition(Token token){
        return partitionPerBound.entrySet().stream()
                .filter(boundsIntegerEntry -> boundsIntegerEntry.getKey().contains(token))
                .collect(Collectors.toList()).get(0).getValue();
    }

    /** {@link PartitionerOnVirtualNode} builder. */
    static class Builder implements Partitioner.Builder<PartitionerOnVirtualNode>{

        private final int vnodes_per_partition;

        Builder(@JsonProperty("vnodes_per_partition") int vnodes_per_partition) {
            this.vnodes_per_partition = vnodes_per_partition;
        }


        @Override
        public PartitionerOnVirtualNode build(CFMetaData metaData) {
            return new PartitionerOnVirtualNode(vnodes_per_partition
                    , StorageService.instance.getLocalTokens().stream().sorted().collect(Collectors.toList()));
        }
    }

}
