package com.stratio.cassandra.lucene.partitioning;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.stratio.cassandra.lucene.common.JsonSerializer;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**Class defining an index partitioning strategy. Partitioning splits each node index in multiple
 * partitions in order to speed up some searches to the detriment of others, depending on
 * the concrete partitioning strategy. It is also useful to overcome the  Lucene's hard limit of
 * 2147483519 documents per local index, which becomes a per-partition limit.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public interface Partitioner {


    /** Returns the number of partitions. */
    int numPartitions();

    /** Returns all the partitions. */
    List<Integer> allPartitions();

    /** Returns the partition for the specified partition key.
     *
     * @param key a partition key to be routed to a partition
     * @return the partition owning `key`
     */
    int partition(DecoratedKey key);

    /** Returns the involved partitions for the specified read command.
     *
     * @param command a read command to be routed to some partitions
     * @return the partitions containing the all data required to satisfy `command`
     */
    List<Integer> partitions(ReadCommand command);




    /** Returns the {@link Builder} represented by the specified JSON string.
     *
     * @param json a JSON string representing a [[Partitioner]]
     * @return the partitioner builder represented by `json`
     */
    static Builder fromJson(String json){
        try {
            return JsonSerializer.fromString(json, Builder.class);
        } catch (IOException e) {
            throw  new RuntimeException(e);
        }
    }

    /** Returns the {@link Partitioner} represented by the specified JSON string.
     *
     * @param metaData the indexed table metadata
     * @param json a JSON string representing a [[Partitioner]]
     * @return the partitioner represented by `json`
     */
    static Partitioner fromJson(CFMetaData metaData, String json){
        return fromJson(json).build(metaData);
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "type",
            defaultImpl = PartitionerOnNone.Builder.class)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = PartitionerOnNone.Builder.class, name = "none"),
            @JsonSubTypes.Type(value = PartitionerOnToken.Builder.class, name = "token"),
            @JsonSubTypes.Type(value = PartitionerOnColumn.Builder.class, name = "column"),
            @JsonSubTypes.Type(value = PartitionerOnVirtualNode.Builder.class, name = "vnode")})
    interface Builder<T extends Partitioner>{
         T build(CFMetaData metaData);
    }

}
