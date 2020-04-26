package com.stratio.cassandra.lucene;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.index.DocumentIterator;
import com.stratio.cassandra.lucene.mapping.ClusteringMapper;
import com.stratio.cassandra.lucene.mapping.KeyMapper;
import com.stratio.cassandra.lucene.mapping.PartitionMapper;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * {@link IndexService} for wide rows.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 **/
public class IndexServiceWide extends IndexService {


    public final ClusteringMapper clusteringMapper;
    public final KeyMapper keyMapper;

    /**
     * @param table the indexed table
     * @param index the index metadata
     */
    public IndexServiceWide(ColumnFamilyStore table, IndexMetadata index) {
        super(table, index);
        clusteringMapper = new ClusteringMapper(super.metaData);
        keyMapper = new KeyMapper(super.metaData);
        init();
    }

    @Override
    public List<SortField> keySortFields() {
        return Lists.newArrayList(tokenMapper.sortField(), partitionMapper.sortField(), clusteringMapper.sortField());
    }

    @Override
    public Set<String> fieldsToLoad() {
        return Sets.newHashSet(PartitionMapper.FIELD_NAME, ClusteringMapper.FIELD_NAME);
    }


    /**
     * Returns the clustering key contained in the specified document.
     *
     * @param document a document containing the clustering key to be get
     * @return the clustering key contained in `document`
     */
    public Clustering clustering(Document document) {
        return clusteringMapper.clustering(document);
    }


    @Override
    public List<IndexableField> keyIndexableFields(DecoratedKey key, Clustering clustering) {
        List<IndexableField> fields = new ArrayList<>();
        fields.add(tokenMapper.indexableField(key));
        fields.add(partitionMapper.indexableField(key));
        fields.add(keyMapper.indexableField(key, clustering));
        fields.addAll(clusteringMapper.indexableFields(key, clustering));
        return fields;
    }

    @Override
    public Term term(DecoratedKey key, Clustering clustering) {
        return keyMapper.term(key, clustering);
    }

    @Override
    public IndexWriter writer(DecoratedKey key, int nowInSec, OpOrder.Group orderGroup, IndexTransaction.Type transactionType) {
        return new IndexWriterWide(this, key, nowInSec, orderGroup, transactionType);
    }

    @Override
    public boolean doesAffectIndex(Row row) {
        return !row.isStatic() && super.doesAffectIndex(row);
    }

    @Override
    public Query query(DecoratedKey key, ClusteringIndexFilter filter) {
        if (filter.selectsAllPartition()) {
            return partitionMapper.query(key);
        } else if (filter instanceof ClusteringIndexNamesFilter) {
            return keyMapper.query(key, (ClusteringIndexNamesFilter) filter);
        } else if (filter instanceof ClusteringIndexSliceFilter) {
            return clusteringMapper.query(key, (ClusteringIndexSliceFilter) filter);
        } else {
            throw new IndexException("Unknown filter type {}", filter.getClass());
        }
    }

    public Query query(PartitionPosition position) {
        if (position instanceof DecoratedKey) {
            return partitionMapper.query((DecoratedKey) position);
        } else {
            return tokenMapper.query(position.getToken());
        }
    }

    public Query query(PartitionPosition position, Optional<ClusteringPrefix> start, Optional<ClusteringPrefix> stop) {
        if (!start.isPresent() && !stop.isPresent()) {
            return query(position);
        } else {
            return new BooleanQuery.Builder()
                    .add(query(position), BooleanClause.Occur.FILTER)
                    .add(clusteringMapper.query(position, start, stop), BooleanClause.Occur.FILTER)
                    .build();
        }
    }

    @Override
    public Optional<Query> query(DataRange dataRange) {

        // Check trivial
        if (dataRange.isUnrestricted()) return Optional.empty();

        // Extract data range data
        PartitionPosition startPosition = dataRange.startKey();
        PartitionPosition stopPosition = dataRange.stopKey();
        Token startToken = startPosition.getToken();
        Token stopToken = stopPosition.getToken();
        Optional<ClusteringPrefix> startClustering = ClusteringMapper.startClusteringPrefix(dataRange).filter(clusteringPrefix -> clusteringPrefix.size() > 0);
        Optional<ClusteringPrefix> stopClustering = ClusteringMapper.stopClusteringPrefix(dataRange).filter(clusteringPrefix -> clusteringPrefix.size() > 0);


        // Try single partition
        if (startToken.compareTo(stopToken) == 0) {
            if (!startClustering.isPresent() && !stopClustering.isPresent()) {
                return Optional.ofNullable(query(startPosition));
            } else {
                return Optional.ofNullable(query(startPosition, startClustering, stopClustering));
            }
        }

        // Prepare query builder
        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        // Add token range filter
        boolean includeStartToken = startPosition.kind().equals(PartitionPosition.Kind.MIN_BOUND) && !startClustering.isPresent();
        boolean includeStopToken = stopPosition.kind().equals(PartitionPosition.Kind.MAX_BOUND) && !stopClustering.isPresent();
        tokenMapper.query(startToken, stopToken, includeStartToken, includeStopToken)
                .ifPresent(query -> builder.add(query, BooleanClause.Occur.SHOULD));

        // Add first and last partition filters
        if (startClustering.isPresent())
            builder.add(query(startPosition, startClustering, Optional.empty()), BooleanClause.Occur.SHOULD);
        if (stopClustering.isPresent())
            builder.add(query(stopPosition, Optional.empty(), stopClustering), BooleanClause.Occur.SHOULD);

        // Return query, or empty if there are no restrictions
        BooleanQuery booleanQuery = builder.build();
        return booleanQuery.clauses().isEmpty() ? Optional.empty() : Optional.of(booleanQuery);
    }

    @Override
    public Term after(DecoratedKey key, Clustering clustering) {
        return keyMapper.term(key, clustering);
    }

    @Override
    public IndexReader reader(DocumentIterator documents, ReadCommand command, ReadExecutionController controller) {
        return new IndexReaderWide(this, command, table, controller, documents);
    }

    public void delete(DecoratedKey key, Slice slice) {
        queue.submitAsynchronous(key, () -> {
            int partition = partitioner.partition(key);
            Query query = clusteringMapper.query(key, slice);
            lucene.delete(partition, query);
            return lucene;
        });
    }
}
