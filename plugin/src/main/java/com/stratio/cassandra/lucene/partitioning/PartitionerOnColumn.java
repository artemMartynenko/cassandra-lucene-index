package com.stratio.cassandra.lucene.partitioning;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** {@link Partitioner} based on a partition key column. Rows will be stored in an index partition
 * determined by the hash of the specified partition key column. Both partition-directed and token
 * range searches containing an CQL equality filter over the selected partition key column will be
 * routed to a single partition, increasing performance. However, token range searches without
 * filters over the partitioning column will be routed to all the partitions, with a slightly lower
 * performance.
 *
 * Load balancing depends on the cardinality and distribution of the values of the partitioning
 * column. Both high cardinalities and uniform distributions will provide better load balancing
 * between partitions.
 *
 * @author Artem Martynenko artem7mag@gmai.com
 * @author Andres de la Pena `adelapena@stratio.com`
 **/
public class PartitionerOnColumn implements Partitioner{

    private final int partitions;
    private final String column;
    private final int position;
    private final AbstractType<?> keyValidator;
    private final List<Integer> allPartitions;

    /**
     * @param partitions   the number of index partitions per node
     * @param column       the name of the partition key column
     * @param position     the position of the partition column in the partition key
     * @param keyValidator the type of the partition key
     * */
    public PartitionerOnColumn(int partitions, String column, int position, AbstractType<?> keyValidator) {
        if (partitions <= 0)
            throw new IndexException("The number of partitions should be strictly positive but found "+partitions);

        if (StringUtils.isBlank(column))
            throw new IndexException("A partition column should be specified");

        if (position < 0)
            throw new IndexException("The column position in the partition key should be positive");

        if (keyValidator == null)
            throw new IndexException("The partition key type should be specified");

        this.partitions = partitions;
        this.column = column;
        this.position = position;
        this.keyValidator = keyValidator;
        this.allPartitions = IntStream.range(0, partitions).boxed().collect(Collectors.toList());
    }


    private int partition(ByteBuffer bb){
        long[] hash = new long[2];
        MurmurHash.hash3_x64_128(bb, bb.position(), bb.remaining(), 0, hash);
        return (int) (Math.abs(hash[0]) % partitions);
    }

    /** {@inheritDoc} */
    @Override
    public int numPartitions() {
        return partitions;
    }

    /** {@inheritDoc} */
    @Override
    public List<Integer> allPartitions() {
        return allPartitions;
    }

    /** {@inheritDoc} */
    @Override
    public int partition(DecoratedKey key) {
        return partition(ByteBufferUtils.split(key.getKey(), keyValidator)[position]);
    }

    /** {@inheritDoc} */
    @Override
    public List<Integer> partitions(ReadCommand command) {
        if(command instanceof SinglePartitionReadCommand){
            Integer partition = partition(((SinglePartitionReadCommand) command).partitionKey());
            return Lists.newArrayList(partition);
        }else if(command instanceof PartitionRangeReadCommand){
            DataRange range = ((PartitionRangeReadCommand) command).dataRange();
            PartitionPosition start = range.startKey();
            PartitionPosition stop = range.stopKey();
            if(start.kind().equals(PartitionPosition.Kind.ROW_KEY)
                    && stop.kind().equals(PartitionPosition.Kind.ROW_KEY)
                    && !start.isMinimum()
                    && !start.equals(stop)){
                return Lists.newArrayList(partition((DecoratedKey) start));
            }else {
                List<RowFilter.Expression> expressions = command.rowFilter().getExpressions();
                Optional<RowFilter.Expression> expression = expressions.stream().filter(exp -> !exp.isCustom())
                        .filter(exp -> exp.column().name.toString().equals(column)).findAny();
                return expression.map(RowFilter.Expression::getIndexValue)
                        .map(this::partition)
                        .map(integer -> (List<Integer>)Lists.newArrayList(integer))
                        .orElse(allPartitions);
            }
        }else {
            throw new IndexException("Unsupported read command type: "+command.getClass());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionerOnColumn that = (PartitionerOnColumn) o;
        return partitions == that.partitions &&
                position == that.position &&
                Objects.equals(column, that.column) &&
                Objects.equals(keyValidator, that.keyValidator) &&
                Objects.equals(allPartitions, that.allPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitions, column, position, keyValidator, allPartitions);
    }

    static class Builder implements Partitioner.Builder<PartitionerOnColumn>{

        private final int partitions;
        private final String column;

        /** {@link PartitionerOnColumn} builder.
         *
         * @param partitions the number of index partitions per node
         * @param column     the name of the partition key column
         */
        Builder(@JsonProperty("partitions") int partitions, @JsonProperty("column") String column) {
            this.partitions = partitions;
            this.column = column;
        }

        @Override
        public  PartitionerOnColumn build(CFMetaData metaData) {
            ByteBuffer name = UTF8Type.instance.decompose(column);
            ColumnDefinition definition = metaData.getColumnDefinition(name);
            if(definition == null){
                throw new IndexException("Partitioner's column '"+column+"' not found in table schema");
            }else if(definition.isPartitionKey()){
                return new PartitionerOnColumn(partitions, column, definition.position(), metaData.getKeyValidator());
            }else {
                throw new IndexException("Partitioner's column '"+column+"' is not part of partition key");
            }
        }

    }
}
