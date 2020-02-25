package com.stratio.cassandra.lucene.index;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.function.Function;

/** Class wrapping a Lucene file system-based directory and its readers, writers and searchers.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class FSIndex {

    static {
        // Disable max boolean query clauses limit
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    }

    /**
     *
     * @param name           the index name
     * @param path           the directory path
     * @param analyzer       the index writer analyzer
     * @param refreshSeconds the index reader refresh frequency in seconds
     * @param ramBufferMB    the index writer RAM buffer size in MB
     * @param maxMergeMB     the directory max merge size in MB
     * @param maxCachedMB    the directory max cache size in MB
     */
    private String name;
    private Path path;
    private Analyzer analyzer;
    private Double refreshSeconds;
    private Integer ramBufferMb;
    private Integer maxMergeMb;
    private Integer maxCacheMb;

    private Sort mergeSort;
    private Set<String> fields;
    private Directory directory;
    private IndexWriter writer;
    private SearcherManager manager;
    private ControlledRealTimeReopenThread<IndexSearcher> reopener;

    public FSIndex(String name,
                   Path path,
                   Analyzer analyzer,
                   Double refreshSeconds,
                   Integer ramBufferMb,
                   Integer maxMergeMb,
                   Integer maxCacheMb) {
        this.name = name;
        this.path = path;
        this.analyzer = analyzer;
        this.refreshSeconds = refreshSeconds;
        this.ramBufferMb = ramBufferMb;
        this.maxMergeMb = maxMergeMb;
        this.maxCacheMb = maxCacheMb;
    }


    /** Initializes this index with the specified merge sort and fields to be loaded.
     *
     * @param mergeSort the sort to be applied to the index during merges
     * @param fields    the names of the document fields to be loaded
     */
    public void init(Sort mergeSort, Set<String> fields){
        this.mergeSort = mergeSort;
        this.fields = fields;

        try {
            // Open or create directory
            directory = new NRTCachingDirectory(FSDirectory.open(path), maxMergeMb, maxCacheMb);

            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            indexWriterConfig.setRAMBufferSizeMB(ramBufferMb);
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            indexWriterConfig.setUseCompoundFile(true);
            indexWriterConfig.setMergePolicy(new SortingMergePolicy(new TieredMergePolicy(), mergeSort));

            writer = new IndexWriter(directory, indexWriterConfig);

            // Setup NRT search
            SearcherFactory searcherFactory = new SearcherFactory(){
                @Override
                public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) throws IOException {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    searcher.setSimilarity(new NoIDFSimilarity());
                    return searcher;
                }
            };

            TrackingIndexWriter tracker = new TrackingIndexWriter(writer);
            manager = new SearcherManager(writer, true, searcherFactory);
            reopener = new ControlledRealTimeReopenThread<>(tracker, manager, refreshSeconds, refreshSeconds);
            reopener.start();

        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private <A> A doWithSearcher(Function<IndexSearcher, A> f) throws IOException {
        IndexSearcher searcher = manager.acquire();
        try {
           return f.apply(searcher);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            manager.release(searcher);
        }
    }

    public SearcherManager searcherManager() {
        return manager;
    }


    /** Upserts the specified document by first deleting the documents containing the specified term
     * and then adding the new document. The delete and then add are atomic as seen by a reader on
     * the same index (flush may happen only after the addition).
     *
     * @param term     the term to identify the document(s) to be deleted
     * @param document the document to be added
     */
    public void upsert(Term term, Document document){
        try {
            writer.updateDocument(term, document);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Deletes all the documents containing the specified term.
     *
     * @param term the term identifying the documents to be deleted
     */
    public void delete(Term term){
        try {
            writer.deleteDocuments(term);
        } catch (IOException e) {
            throw  new RuntimeException(e);
        }
    }

    /** Deletes all the documents satisfying the specified query.
     *
     * @param query the query identifying the documents to be deleted
     */
    public void delete(Query query){
        try {
            writer.deleteDocuments(query);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Deletes all the documents. */
    public void truncate(){
        try {
            writer.deleteAll();
            writer.commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Commits the pending changes. */
    public void commit(){
        try {
            writer.commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /** Commits all changes to the index, waits for pending merges to complete, and closes all
     * associated resources.
     */
    public void close(){

        try {
            reopener.close();
            manager.close();
            writer.close();
            directory.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /** Closes the index and removes all its files. */
    public void delete(){
        try {
            close();
        }finally {
            FileUtils.deleteRecursive(path.toFile());
        }
    }

    /** Returns the total number of documents in this index.
     *
     * @return the number of documents
     */
    public Integer getNumDocs(){
        try {
            return doWithSearcher(searcher -> searcher.getIndexReader().numDocs());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /** Returns the total number of deleted documents in this index.
     *
     * @return the number of deleted documents
     */
    public Integer getNumDeletedDocs(){
        try {
           return doWithSearcher(searcher -> searcher.getIndexReader().numDeletedDocs());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Optimizes the index forcing merge segments leaving the specified number of segments.
     * This operation may block until all merging completes.
     *
     * @param maxNumSegments the maximum number of segments left in the index after merging finishes
     * @param doWait         `true` if the call should block until the operation completes
     */
    public void forceMerge(Integer maxNumSegments, Boolean doWait){
        try {
            writer.forceMerge(maxNumSegments, doWait);
            writer.commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /** Optimizes the index forcing merge of all segments that have deleted documents.
     * This operation may block until all merging completes.
     *
     * @param doWait `true` if the call should block until the operation completes
     */
    public void forceMergeDeletes(Boolean doWait){
        try {
            writer.forceMergeDeletes(doWait);
            writer.commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Refreshes the index readers. */
    public void refresh(){
        try {
            manager.maybeRefreshBlocking();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }




}
