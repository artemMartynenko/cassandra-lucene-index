package com.stratio.cassandra.lucene.mapping;


import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class PartitionSort extends SortField {


    public PartitionSort(PartitionMapper mapper) {
        super(PartitionMapper.FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                return new FieldComparator.TermValComparator(numHits, fieldname, false){
                    @Override
                    public int compareValues(BytesRef val1, BytesRef val2) {
                        ByteBuffer bb1 = ByteBufferUtils.byteBuffer(val1);
                        ByteBuffer bb2 = ByteBufferUtils.byteBuffer(val2);
                        return mapper.validator.compare(bb1, bb2);
                    }
                };
            }
        });
    }

    @Override
    public String toString() {
        return "<partition>";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PartitionSort;
    }
}
