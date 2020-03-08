package com.stratio.cassandra.lucene.index;


import com.stratio.cassandra.lucene.util.ThrowingWrapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.stratio.cassandra.lucene.util.ThrowingWrapper.wrapc;
import static com.stratio.cassandra.lucene.util.ThrowingWrapper.wrapf;

/** Class wrapping a Lucene RAM directory and its readers, writers and searchers for NRT.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 */

public class RAMIndex {


    private final RAMDirectory directory;
    private final IndexWriter indexWriter;
    private final Analyzer analyzer;


    /**
      * @param analyzer the index writer analyzer
      * */
    public RAMIndex(Analyzer analyzer) {
        this.analyzer = analyzer;
        this.directory = new RAMDirectory();
        try {
            this.indexWriter  = new IndexWriter(directory, new IndexWriterConfig(analyzer));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Adds the specified document.
      *
      * @param document the document to be added
      */
    public void add(Document document){
        try {
            indexWriter.addDocument(document);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Commits all pending changes to the index, waits for pending merges to complete, and closes all
      * associated resources.
      */
    public void close(){
        try {
            indexWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            directory.close();
        }
    }


    /** Finds the top count hits for a query and a sort.
      *
      * @param query  the query to search for
      * @param sort   the sort to be applied
      * @param count  the max number of results to be collected
      * @param fields the names of the fields to be loaded
      * @return the found documents
      */
    public List<Tuple<Document, ScoreDoc>> search(Query query, Sort sort, int count, Set<String> fields){
        DirectoryReader reader;
        try {
            indexWriter.commit();
            reader = DirectoryReader.open(directory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        IndexSearcher searcher = new IndexSearcher(reader);

        try {
            Sort newSort = sort.rewrite(searcher);
            TopDocs topDocs = searcher.search(query, count, newSort, true, true);
            return Arrays.stream(topDocs.scoreDocs)
                    .map(wrapf(scoreDoc -> new Tuple(searcher.doc(scoreDoc.doc, fields), scoreDoc)))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            wrapc((IndexSearcher s) -> s.getIndexReader().close()).accept(searcher);
        }
    }

}
