package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.index.DocumentIterator;
import com.stratio.cassandra.lucene.index.Tuple;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;

import java.util.Optional;

/** {@link IndexReader} for skinny rows.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 **/
public class IndexReaderSkinny extends IndexReader{

    protected final IndexServiceSkinny service;

    /**
     * @param service    the index service
     * @param command    the read command
     * @param table      the base table
     * @param controller the read execution controller
     * @param documents  the documents iterator
     */
    public IndexReaderSkinny(IndexServiceSkinny service, ReadCommand command, ColumnFamilyStore table, ReadExecutionController controller, DocumentIterator documents) {
        super(command, table, controller, documents);
        this.service = service;
    }

    @Override
    protected boolean prepareNext() {
        while (!nextData.isPresent() && documents.hasNext()){
            Tuple<Document, ScoreDoc> nextDoc = documents.next();
            DecoratedKey key = service.decoratedKey(nextDoc._1);
            ClusteringIndexFilter filter = command.clusteringIndexFilter(key);
            nextData = Optional.of(read(key, filter));
            nextData.ifPresent(unfilteredRowIterator -> {
                if (unfilteredRowIterator.isEmpty()){
                    unfilteredRowIterator.close();
                }
            });
        }
        return nextData.isPresent();
    }
}
