package com.stratio.cassandra.lucene.mapping;

import com.google.common.base.MoreObjects;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

import static com.stratio.cassandra.lucene.mapping.ClusteringMapper.PREFIX_SIZE;
import static org.apache.cassandra.utils.FastByteOperations.compareUnsigned;


/**
 * {@link MultiTermQuery} to get a range of clustering keys.
 * @author Artem Martynenko artem7mag@gmail.com
 */
public class ClusteringQuery extends MultiTermQuery {

    private final ClusteringMapper mapper;
    private final PartitionPosition position;
    private final Optional<ClusteringPrefix> start;
    private final Optional<ClusteringPrefix> stop;

    private final Token token;
    private final byte[] seek;
    private final ClusteringComparator comparator;

    /**
     * @param mapper   the clustering key mapper to be used
     * @param position the partition position
     * @param start    the start clustering
     * @param stop     the stop clustering
     */
    public ClusteringQuery(ClusteringMapper mapper, PartitionPosition position, ClusteringPrefix start, ClusteringPrefix stop) {
        super(ClusteringMapper.FIELD_NAME);
        this.mapper = mapper;
        this.position = position;
        this.start = Optional.ofNullable(start);
        this.stop = Optional.ofNullable(stop);
        this.token = position.getToken();
        this.seek = ClusteringMapper.prefix(token);
        this.comparator = mapper.comparator;
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource attributeSource) throws IOException {
       return new FullKeyDataRangeFilteredTermsEnum(terms.iterator());
    }

    /** Important to avoid collisions in Lucene's query cache. */
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof ClusteringQuery){
            ClusteringQuery q = (ClusteringQuery)obj;
            return  token.equals(q.token) && start.equals(q.start) && stop.equals(q.stop);
        }else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + token.hashCode();
        result = 31 * result + start.map(ClusteringPrefix::hashCode).orElse(0);
        result = 31 * result + stop.map(ClusteringPrefix::hashCode).orElse(0);
        return result;
    }

    @Override
    public String toString(String field) {
        return MoreObjects.toStringHelper(this)
                .add("field", field)
                .add("token", token)
                .add("start", mapper.toString(start))
                .add("stop", mapper.toString(stop))
                .toString();
    }


    class FullKeyDataRangeFilteredTermsEnum extends FilteredTermsEnum {


        public FullKeyDataRangeFilteredTermsEnum(TermsEnum tenum) {
            super(tenum);
            // Jump to the start of the partition
            setInitialSeekTerm(new BytesRef(seek));
        }


        @Override
        protected AcceptStatus accept(BytesRef term) throws IOException {
            int comp = compareUnsigned(term.bytes, 0, PREFIX_SIZE, seek, 0, PREFIX_SIZE);
            if (comp < 0) return AcceptStatus.NO;
            if (comp > 0) return AcceptStatus.END;

            ByteBuffer bb = ByteBuffer.wrap(term.bytes, PREFIX_SIZE, term.length - PREFIX_SIZE);
            Clustering clustering = mapper.clustering(bb);
            if(start.filter(clusteringPrefix -> comparator.compare(clusteringPrefix, clustering) > 0).isPresent()) return AcceptStatus.NO;
            if(start.filter(clusteringPrefix -> comparator.compare(clusteringPrefix, clustering) < 0).isPresent()) return AcceptStatus.NO;
            return AcceptStatus.YES;
        }
    }

}
