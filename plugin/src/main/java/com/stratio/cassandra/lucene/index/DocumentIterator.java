package com.stratio.cassandra.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**[[CloseableIterator]] for retrieving Lucene documents satisfying a query.
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class DocumentIterator implements Iterator<Tuple<Document, ScoreDoc>>, AutoCloseable {

    private static int MAX_PAGE_SIZE = 10000;


    private  List<Tuple<SearcherManager, Term>> cursors;
    private Sort indexSort;
    private Sort querySort;
    private Query query;
    private Integer limit;
    private Set<String> fields;

    private int pageSize;
    private LinkedList<Tuple<Document, ScoreDoc>>documents;
    private  List<SearcherManager> managers;
    private  List<Integer> indices;
    private  List<IndexSearcher> searchers;
    private  List<Term> afterTerms;
    private  List<Integer> offsets;
    private  boolean finished;
    private  boolean closed;


    /**
     * @param cursors   the searcher managers and pointers of the involved indexes
     * @param indexSort the sort of the index
     * @param querySort the sort in which the documents are going to be retrieved
     * @param query     the query to be satisfied by the documents
     * @param limit     the iteration page size
     * @param fields    the names of the document fields to be loaded
     */
    public DocumentIterator(List<Tuple<SearcherManager, Term>> cursors, Sort indexSort, Sort querySort, Query query, Integer limit, Set<String> fields) {
        this.cursors = cursors;
        this.indexSort = indexSort;
        this.querySort = querySort;
        this.query = query;
        this.limit = limit;
        this.fields = fields;

        this.pageSize =  Math.min(limit, MAX_PAGE_SIZE) + 1 ;
        this.documents =  new LinkedList<>();
        this.indices = IntStream.range(0, cursors.size()).boxed().collect(Collectors.toList());
        this.managers =  cursors.stream()
                .map(searcherManagerTermTuple -> searcherManagerTermTuple._1)
                .collect(Collectors.toList());

        this.searchers = managers.stream()
                .map(searcherManager -> {
                    try {
                        return searcherManager.acquire();
                    } catch (IOException e) {
                        return null;
                    }
                }).collect(Collectors.toList());

        this.afterTerms = cursors.stream()
                .map(searcherManagerTermTuple -> searcherManagerTermTuple._2)
                .collect(Collectors.toList());
        this.offsets = cursors.stream().map(searcherManagerTermTuple -> 0).collect(Collectors.toList());
        this.finished = false;
        this.closed = false;
    }


    @Override
    public void close() throws Exception {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Tuple<Document, ScoreDoc> next() {
        return null;
    }
}
