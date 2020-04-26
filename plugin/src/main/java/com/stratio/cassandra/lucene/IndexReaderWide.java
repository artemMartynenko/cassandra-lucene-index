package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.index.DocumentIterator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.lucene.document.Document;

import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;

/**
 * {@link IndexReader} for wide rows.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 **/
public class IndexReaderWide extends IndexReader {

    protected final IndexServiceWide service;
    private final ClusteringComparator comparator;
    private Document nextDoc;

    /**
     * @param service    the index service
     * @param command    the read command
     * @param table      the base table
     * @param controller the read execution controller
     * @param documents  the documents iterator
     */
    public IndexReaderWide(IndexServiceWide service, ReadCommand command, ColumnFamilyStore table, ReadExecutionController controller, DocumentIterator documents) {
        super(command, table, controller, documents);
        this.service = service;
        comparator = service.metaData.comparator;
    }


    private NavigableSet<Clustering> readClusterings(DecoratedKey key) {
        TreeSet<Clustering> clusterings = new TreeSet<>(comparator);
        Clustering clustering = service.clustering(nextDoc);
        Clustering lastClustering = null;

        boolean needContinue = true;
        while (needContinue && nextDoc != null && key.getKey().equals(service.decoratedKey(nextDoc).getKey())
                && (lastClustering == null || comparator.compare(lastClustering, clustering) < 0)) {

            if (command.selectsKey(key) && command.selectsClustering(key, clustering)) {
                lastClustering = clustering;
                clusterings.add(clustering);
            }

            if (documents.hasNext()) {
                nextDoc = documents.next()._1;
                clustering = service.clustering(nextDoc);
            } else {
                nextDoc = null;
            }

            needContinue = !documents.needsFetch();

        }

        return clusterings;
    }

    @Override
    protected boolean prepareNext() {
        if (nextData.isPresent()) return true;

        if (nextDoc == null) {
            if (!documents.hasNext()) return false;
            nextDoc = documents.next()._1;
        }

        DecoratedKey key = service.decoratedKey(nextDoc);
        NavigableSet<Clustering> clusterings = readClusterings(key);

        if (clusterings.isEmpty()) prepareNext();

        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings, false);
        nextData = Optional.of(read(key, filter));

        if (nextData.isPresent()) {
            UnfilteredRowIterator data = nextData.get();
            if (data.isEmpty()) {
                data.close();
                return prepareNext();
            }
        }

        return true;
    }
}
