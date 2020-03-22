package com.stratio.cassandra.lucene.mapping;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;

import java.util.Optional;

/** Class for several token mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class TokenMapper {

    /** The Lucene field name */
    public static final String FIELD_NAME = "_token";


    /** The Lucene field type */
    public static final FieldType FIELD_TYPE;
    static {
        FIELD_TYPE = new FieldType();
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setNumericType(FieldType.NumericType.LONG);
        FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
        FIELD_TYPE.freeze();
    }


    /** Returns the `Long` value of the specified Murmur3 partitioning {@link Token}.
     *
     * @param token a Murmur3 token
     * @return the `token`'s `Long` value
     */
    public static long longValue(Token token){
        return (long) token.getTokenValue();
    }

    /** Returns the {@link BytesRef} indexing value of the specified Murmur3 partitioning [[Token]].
     *
     * @param token a Murmur3 token
     * @return the `token`'s indexing value
     */
    public static BytesRef bytesRef(Token token){
        long value = longValue(token);
        BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();
        NumericUtils.longToPrefixCoded(value, 0 , bytesRefBuilder);
        return bytesRefBuilder.get();
    }

    public TokenMapper() {
        if (! (DatabaseDescriptor.getPartitioner() instanceof Murmur3Partitioner)) {
            throw new IndexException("Only Murmur3 partitioner is supported");
        }
    }



    /** Returns the Lucene {@link IndexableField} associated to the token of the specified row key.
     *
     * @param key the raw partition key to be added
     * @return a indexable field
     */
    public IndexableField indexableField(DecoratedKey key){
        Token token = key.getToken();
        long value= longValue(token);
        return new LongField(FIELD_NAME, value, FIELD_TYPE);
    }



    /** Returns a Lucene {@link SortField} for sorting documents according to the partitioner's order.
     *
     * @return a sort field for sorting by token
     */
    public SortField sortField(){
        return new SortField(FIELD_NAME, SortField.Type.LONG);
    }

    /** Returns a query to find the documents containing a token inside the specified token range.
     *
     * @param lower        the lower token
     * @param upper        the upper token
     * @param includeLower if the lower token should be included
     * @param includeUpper if the upper token should be included
     * @return the query to find the documents containing a token inside the range
     */
    public Optional<Query> query(Token lower,
                                 Token upper,
                                 boolean includeLower,
                                 boolean includeUpper){

        // Skip if it's full data range
        if(lower.isMinimum() && upper.isMinimum()) return Optional.empty();

        // Get token values
        long min = lower.isMinimum() ? Long.MIN_VALUE : longValue(lower);
        long max = upper.isMinimum() ? Long.MAX_VALUE : longValue(upper);

        // Do query using doc values or inverted index depending on empirical heuristic
        if(max / 10 - min / 10 > 1222337203685480000L){
            return Optional.of(DocValuesRangeQuery.newLongRange(FIELD_NAME, min, max, includeLower, includeUpper));
        }else {
            return Optional.of(NumericRangeQuery.newLongRange(FIELD_NAME, min, max, includeLower, includeUpper));
        }

    }



    /** Returns a Lucene query to find the documents containing the specified token.
     *
     * @param token the token
     * @return the query to find the documents containing `token`
     */
    public Query query(Token token){
        return new TermQuery(new Term(FIELD_NAME, bytesRef(token)));
    }
}
