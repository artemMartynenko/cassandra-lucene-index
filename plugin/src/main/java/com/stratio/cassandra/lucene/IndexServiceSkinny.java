package com.stratio.cassandra.lucene;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.index.DocumentIterator;
import com.stratio.cassandra.lucene.mapping.PartitionMapper;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**{@link IndexService} for skinny rows
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 **/
public class IndexServiceSkinny extends IndexService{


    /**
     * @param table the indexed table
     * @param index the index metadata
     * */
    public IndexServiceSkinny(ColumnFamilyStore table, IndexMetadata index) {
        super(table, index);
        init();
    }

    @Override
    List<SortField> keySortFields() {
        return Lists.newArrayList(tokenMapper.sortField(), partitionMapper.sortField());
    }

    @Override
    Set<String> fieldsToLoad() {
        return Sets.newHashSet(PartitionMapper.FIELD_NAME);
    }

    @Override
    List<IndexableField> keyIndexableFields(DecoratedKey key, Clustering clustering) {
        return Lists.newArrayList(tokenMapper.indexableField(key), partitionMapper.indexableField(key));
    }

    @Override
    public Term term(DecoratedKey key, Clustering clustering) {
        return partitionMapper.term(key);
    }

    @Override
    public IndexWriter writer(DecoratedKey key, int nowInSec, OpOrder.Group orderGroup, IndexTransaction.Type transactionType) {
        return new IndexWriterSkinny(this, key, nowInSec, orderGroup, transactionType);
    }

    @Override
    public Query query(DecoratedKey key, ClusteringIndexFilter filter) {
        return new TermQuery(term(key));
    }

    @Override
    public Optional<Query> query(DataRange dataRange) {
        PartitionPosition startKey = dataRange.startKey();
        PartitionPosition stopKey = dataRange.stopKey();

        if(startKey.kind().equals(PartitionPosition.Kind.ROW_KEY) && stopKey.kind().equals(PartitionPosition.Kind.ROW_KEY) && startKey.equals(stopKey)){
            return Optional.ofNullable(partitionMapper.query((DecoratedKey) startKey));
        } else {
            return tokenMapper.query(startKey.getToken(),
                    stopKey.getToken(),
                    startKey.kind().equals(PartitionPosition.Kind.MIN_BOUND),
                    stopKey.kind().equals(PartitionPosition.Kind.MAX_BOUND));
        }
    }

    @Override
    public Term after(DecoratedKey key, Clustering clustering) {
        return partitionMapper.term(key);
    }

    @Override
    public IndexReader reader(DocumentIterator documents, ReadCommand command, ReadExecutionController controller) {
        return new IndexReaderSkinny(this, command, table, controller, documents);
    }

}
