package com.stratio.cassandra.lucene.mapping;

import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;

/** Class for several primary key mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class KeyMapper {

    /** The Lucene field name. */
    public static final String FILED_NAME = "_key";

    public CFMetaData metaData;

    /** The clustering key comparator */
    public final ClusteringComparator clusteringComparator;

    /** A composite type composed by the types of the clustering key */
    public final CompositeType clusteringType;

    /** The type of the primary key, which is composed by token and clustering key types. */
    public final CompositeType keyType;


    /**
     * @param metaData the indexed table metadata
     * */
    public KeyMapper(CFMetaData metaData) {
        this.metaData = metaData;
        clusteringComparator = metaData.comparator;
        clusteringType = CompositeType.getInstance(clusteringComparator.subtypes());
        keyType = CompositeType.getInstance(metaData.getKeyValidator(), clusteringType);
    }



    /** Returns a {@link ByteBuffer} representing the specified clustering key
     *
     * @param clustering the clustering key
     * @return the byte buffer representing `clustering`
     */
    private ByteBuffer byteBuffer(Clustering clustering){
        CompositeType.Builder builder = clusteringType.builder();
        for (ByteBuffer bb: clustering.getRawValues()) {
            builder.add(bb);
        }
        return builder.build();
    }


    /** Returns the Lucene {@link IndexableField} representing the specified primary key.
     *
     * @param key        the partition key
     * @param clustering the clustering key
     * @return a indexable field
     */
    public IndexableField indexableField(DecoratedKey key, Clustering clustering){
        return new StringField(FILED_NAME, bytesRef(key, clustering), Field.Store.NO);
    }


    /** Returns the Lucene term representing the specified primary.
     *
     * @param key        a partition key
     * @param clustering a clustering key
     * @return the Lucene term representing the primary key
     */
    public Term term(DecoratedKey key, Clustering clustering){
        return new Term(FILED_NAME, bytesRef(key, clustering));
    }


    public BytesRef bytesRef(DecoratedKey key, Clustering clustering){
       return ByteBufferUtils.bytesRef(keyType.builder()
               .add(key.getKey())
               .add(byteBuffer(clustering))
               .build());
    }


    /** Returns a Lucene [[Query]] to retrieve the row with the specified primary key.
     *
     * @param key        a partition key
     * @param clustering a clustering key
     * @return the Lucene query
     */
    public Query query(DecoratedKey key, Clustering clustering){
        return new TermQuery(term(key, clustering));
    }



    /** Returns a Lucene [[Query]] to retrieve all the rows in the specified clustering names filter.
     *
     * @param key    a partition key
     * @param filter a names filter
     * @return the Lucene query
     */
    public Query query(DecoratedKey key, ClusteringIndexNamesFilter filter){
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        filter.requestedRows().forEach(clustering -> builder.add(query(key, clustering), BooleanClause.Occur.SHOULD));
        return builder.build();
    }
}
