package com.stratio.cassandra.lucene.mapping;

import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class PartitionMapper {

    /** The Lucene field name. */
    public static final String FIELD_NAME = "_partition";
    public static final FieldType FIELD_TYPE;

    static {
        FIELD_TYPE = new FieldType();
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setStored(true);
        FIELD_TYPE.setDocValuesType(DocValuesType.SORTED);
        FIELD_TYPE.freeze();
    }

    public final CFMetaData metaData;

    public final IPartitioner partitioner;
    public final AbstractType<?> validator;
    public final List<ColumnDefinition> partitionKeyColumns;

    public PartitionMapper(CFMetaData metaData) {
        this.metaData = metaData;
        partitioner = DatabaseDescriptor.getPartitioner();
        validator = metaData.getKeyValidator();
        partitionKeyColumns = metaData.partitionKeyColumns();
    }



    /** Returns the Lucene indexable field representing to the specified partition key.
     *
     * @param partitionKey the partition key to be converted
     * @return a indexable field
     */
    public IndexableField indexableField(DecoratedKey partitionKey){
        ByteBuffer bb = partitionKey.getKey();
        BytesRef bytesRef = ByteBufferUtils.bytesRef(bb);
        return new Field(FIELD_NAME, bytesRef, FIELD_TYPE);
    }


    /** Returns the specified raw partition key as a Lucene term.
     *
     * @param partitionKey the raw partition key to be converted
     * @return a Lucene term
     */
    public Term term(ByteBuffer partitionKey){
        BytesRef bytesRef = ByteBufferUtils.bytesRef(partitionKey);
        return new Term(FIELD_NAME, bytesRef);
    }



    /** Returns the specified raw partition key as a Lucene term.
     *
     * @param partitionKey the raw partition key to be converted
     * @return a Lucene term
     */
    public Term term(DecoratedKey partitionKey){
        return term(partitionKey.getKey());
    }


    /** Returns the specified raw partition key as a Lucene query.
     *
     * @param partitionKey the raw partition key to be converted
     * @return the specified raw partition key as a Lucene query
     */
    public Query query(DecoratedKey partitionKey){
        return new TermQuery(term(partitionKey));
    }




    /** Returns the specified raw partition key as a Lucene query.
     *
     * @param partitionKey the raw partition key to be converted
     * @return the specified raw partition key as a Lucene query
     */
    public Query query(ByteBuffer partitionKey){
        return new TermQuery(term(partitionKey));
    }



    /** Returns the partition key contained in the specified Lucene document.
     *
     * @param document the document containing the partition key to be get
     * @return the key contained in the specified Lucene document
     */
    public DecoratedKey decoratedKey(Document document){
        BytesRef bytesRef = document.getBinaryValue(FIELD_NAME);
        ByteBuffer bb = ByteBufferUtils.byteBuffer(bytesRef);
        return partitioner.decorateKey(bb);
    }

    /** Returns a Lucene sort field for sorting documents/rows according to the partition key.
     *
     * @return a sort field for sorting by partition key
     */
    public SortField sortField(){
        return new PartitionSort(this);
    }
}
