package com.stratio.cassandra.lucene.mapping;


import org.apache.cassandra.db.Clustering;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.stratio.cassandra.lucene.mapping.ClusteringMapper.FIELD_NAME;
import static com.stratio.cassandra.lucene.mapping.ClusteringMapper.PREFIX_SIZE;
import static org.apache.cassandra.utils.FastByteOperations.compareUnsigned;

/**
 * @author Artem Martynenko artem7mag@gmail.com
 */
public class ClusteringSort extends SortField {


    /** {@link SortField} to sort by token and clustering key.
     *
     * @param mapper the primary key mapper to be used
     */
    public ClusteringSort(ClusteringMapper mapper) {
        super(FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                return new FieldComparator.TermValComparator(numHits, fieldname, false){
                    @Override
                    public int compareValues(BytesRef t1, BytesRef t2) {
                        int comp = compareUnsigned(t1.bytes,  0 , PREFIX_SIZE, t2.bytes, 0, PREFIX_SIZE);
                        if(comp != 0) return comp;
                        ByteBuffer bb1 = ByteBuffer.wrap(t1.bytes, PREFIX_SIZE, t1.length - PREFIX_SIZE);
                        ByteBuffer bb2 = ByteBuffer.wrap(t2.bytes, PREFIX_SIZE, t2.length - PREFIX_SIZE);
                        Clustering clustering1 = mapper.clustering(bb1);
                        Clustering clustering2 = mapper.clustering(bb2);
                        return mapper.comparator.compare(clustering1, clustering2);
                    }
                };
            }
        });
    }


    @Override
    public String toString() {
        return "<clustering>";
    }

    @Override
    public boolean equals(Object o) {
       return o instanceof ClusteringSort;
    }

}
