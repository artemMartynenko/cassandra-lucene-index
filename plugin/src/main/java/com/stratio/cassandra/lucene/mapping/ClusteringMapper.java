package com.stratio.cassandra.lucene.mapping;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

/** Class for several clustering key mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 */
public class ClusteringMapper {

    /** The Lucene field name. */
    public static final String FIELD_NAME = "_clustering";


    /** The number of bytes produced by token collation. */
    public static final int PREFIX_SIZE = 8;

    public static final FieldType FIELD_TYPE;

    static {
        FIELD_TYPE = new FieldType();
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.setDocValuesType(DocValuesType.SORTED);
        FIELD_TYPE.freeze();
    }


    /** Returns a lexicographically sortable representation of the specified token.
     *
     * @param token a token
     * @return a lexicographically sortable 8 bytes array
     */
    @SuppressWarnings({"NumericOverflow"})
    public static byte[] prefix(Token token){
        long value = TokenMapper.longValue(token);
        long collated = Long.MIN_VALUE * -1 + value;
        return Longs.toByteArray(collated);
    }



    /** Returns the start {@link ClusteringPrefix} of the first partition of the specified [[DataRange]].
      *
      * @param range the data range
      * @return the optional start clustering prefix of the data range
      */
    public static Optional<ClusteringPrefix> startClusteringPrefix(DataRange range){
       PartitionPosition key = range.startKey();
        ClusteringIndexFilter filter = getFilter(range , key);

        if(filter instanceof ClusteringIndexSliceFilter){
            ClusteringIndexSliceFilter slice  = (ClusteringIndexSliceFilter) filter;
            return Optional.of(slice.requestedSlices().get(0).start());
        } else if(filter instanceof ClusteringIndexNamesFilter){
            ClusteringIndexNamesFilter slice  = (ClusteringIndexNamesFilter) filter;
            return Optional.of(slice.requestedRows().first());
        }else {
            return Optional.empty();
        }
    }

    public static Optional<ClusteringPrefix> stopClusteringPrefix(DataRange range){
        PartitionPosition key = range.stopKey();
        ClusteringIndexFilter filter = getFilter(range, key);

        if(filter instanceof ClusteringIndexSliceFilter){
            ClusteringIndexSliceFilter slice  = (ClusteringIndexSliceFilter) filter;
            return Optional.of(slice.requestedSlices().get(slice.requestedSlices().size() - 1).end());
        } else if(filter instanceof ClusteringIndexNamesFilter){
            ClusteringIndexNamesFilter slice  = (ClusteringIndexNamesFilter) filter;
            return Optional.of(slice.requestedRows().last());
        }else {
            return Optional.empty();
        }
    }


    private static ClusteringIndexFilter getFilter(DataRange range , PartitionPosition key){
        if(key instanceof DecoratedKey){
            return range.clusteringIndexFilter((DecoratedKey) key);
        }else {
            return range.clusteringIndexFilter(new BufferDecoratedKey(key.getToken(), ByteBufferUtil.EMPTY_BYTE_BUFFER));
        }
    }

    public final CFMetaData metaData;

    /** The clustering key comparator */
    public final ClusteringComparator comparator;

    /** A composite type composed by the types of the clustering key */
    public final CompositeType clusteringType;

    public final List<ColumnDefinition> clusteringColumns;


   /**
    * @param metaData the indexed table metadata
    * */
    public ClusteringMapper(CFMetaData metaData) {
        this.metaData = metaData;
        this.comparator = metaData.comparator;
        this.clusteringType = CompositeType.getInstance(comparator.subtypes());
        this.clusteringColumns = metaData.clusteringColumns();
    }


    /** Returns a list of Lucene {@link IndexableField}s representing the specified primary key.
      *
      * @param key        the partition key
      * @param clustering the clustering key
      * @return a indexable field
      */
    public List<IndexableField> indexableFields(DecoratedKey key, Clustering clustering){
        BytesRef plainClustering = ByteBufferUtils.bytesRef(byteBuffer(clustering));
        StoredField storedField = new StoredField(FIELD_NAME, plainClustering);

        ByteBuffer bb = ByteBuffer.allocate(PREFIX_SIZE + plainClustering.length);
        bb.put(prefix(key.getToken())).put(plainClustering.bytes).flip();
        Field indexedField  = new Field(FIELD_NAME, ByteBufferUtils.bytesRef(bb), FIELD_TYPE);

        return Lists.newArrayList(indexedField, storedField);
    }


    /** Returns the {@link ByteBuffer} representation of the specified {@link Clustering}.
      *
      * @param clustering a clustering key
      * @return a byte buffer representing `clustering`
      *
      */
    public ByteBuffer byteBuffer(Clustering clustering){
      CompositeType.Builder builder =  clusteringType.builder();
      for (ByteBuffer bb :clustering.getRawValues()){
          builder.add(bb);
      }
      return builder.build();
    }



    /** Returns the {@link String} human-readable representation of the specified {@link ClusteringPrefix}.
     *
     * @param prefix the clustering prefix
     * @return a {@link String} representing the prefix
     */
    public String toString(Optional<ClusteringPrefix> prefix){
        return prefix.map(clusteringPrefix -> clusteringPrefix.toString(metaData)).orElse( null);
    }


    /** Returns the clustering key represented by the specified [[ByteBuffer]].
      *
      * @param clustering a byte buffer containing a [[Clustering]]
      * @return a Lucene field binary value
      */
    public Clustering clustering(ByteBuffer clustering){
        return Clustering.make(clusteringType.split(clustering));
    }

    /** Returns the clustering key contained in the specified {@link Document}.
      *
      * @param document a document containing the clustering key to be get
      * @return the clustering key contained in the document
      */
    public Clustering clustering(Document document){
        BytesRef bytesRef = document.getBinaryValue(FIELD_NAME);
        return clustering(ByteBufferUtils.byteBuffer(bytesRef));
    }

    /** Returns a Lucene {@link SortField} to sort documents by primary key.
     *
     * @return the sort field
     */
    public SortField sortField(){
        return new ClusteringSort(this);
    }



    /** Returns a Lucene {@link Query} to retrieve all the rows in the specified partition slice.
     *
     * @param position the partition position
     * @param start    the start clustering prefix
     * @param stop     the stop clustering prefix
     * @return the Lucene query
     */
    public Query query(PartitionPosition position,
                       ClusteringPrefix start,
                       ClusteringPrefix stop){
        return new ClusteringQuery(this, position, start, stop);
    }


    /** Returns a Lucene {@link Query} to retrieve all the rows in the specified clustering slice.
     *
     * @param key   the partition key
     * @param slice the slice
     * @return the Lucene query
     */
    public Query query(DecoratedKey key, Slice slice){
        return query(key, slice.start(), slice.end());
    }



    /** Returns a Lucene {@link Query} to retrieve all the rows in the specified clustering slice filter.
     *
     * @param key    the partition key
     * @param filter the slice filter
     * @return the Lucene query
     */
    public Query query(DecoratedKey key, ClusteringIndexSliceFilter filter){
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for(Slice slice: filter.requestedSlices()){
            builder.add(query(key, slice), BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }


}
