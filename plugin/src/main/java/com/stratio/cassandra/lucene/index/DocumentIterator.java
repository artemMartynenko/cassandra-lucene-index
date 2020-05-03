package com.stratio.cassandra.lucene.index;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.stratio.cassandra.lucene.util.ThrowingWrapper.wrapc;
import static com.stratio.cassandra.lucene.util.ThrowingWrapper.wrapf;

/**
 * [[CloseableIterator]] for retrieving Lucene documents satisfying a query.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class DocumentIterator implements Iterable<Tuple<Document, ScoreDoc>>,Iterator<Tuple<Document, ScoreDoc>>, AutoCloseable {

    private static int MAX_PAGE_SIZE = 10000;


    private List<Tuple<SearcherManager, Optional<Term>>> cursors;
    private Sort indexSort;
    private Sort querySort;
    private Query query;
    private Integer limit;
    private Set<String> fields;

    private int pageSize;
    private LinkedList<Tuple<Document, ScoreDoc>> documents;
    private List<SearcherManager> managers;
    private List<Integer> indices;
    private List<IndexSearcher> searchers;
    private List<Optional<Term>> afterTerms;
    private List<Integer> offsets;
    private boolean finished;
    private boolean closed;
    private List<Optional<ScoreDoc>> afters;
    private Sort sort;


    /**
     * @param cursors   the searcher managers and pointers of the involved indexes
     * @param indexSort the sort of the index
     * @param querySort the sort in which the documents are going to be retrieved
     * @param query     the query to be satisfied by the documents
     * @param limit     the iteration page size
     * @param fields    the names of the document fields to be loaded
     */
    public DocumentIterator(List<Tuple<SearcherManager, Optional<Term>>> cursors, Sort indexSort, Sort querySort, Query query, Integer limit, Set<String> fields) {
        this.cursors = cursors;
        this.indexSort = indexSort;
        this.querySort = querySort;
        this.query = query;
        this.limit = limit;
        this.fields = fields;

        this.pageSize = Math.min(limit, MAX_PAGE_SIZE) + 1;
        this.documents = new LinkedList<>();
        this.indices = IntStream.range(0, cursors.size()).boxed().collect(Collectors.toList());
        this.managers = cursors.stream()
                .map(searcherManagerTermTuple -> searcherManagerTermTuple._1)
                .collect(Collectors.toList());

        this.searchers = managers.stream()
                .map(wrapf(SearcherManager::acquire)).collect(Collectors.toList());

        this.afterTerms = cursors.stream()
                .map(searcherManagerTermTuple -> searcherManagerTermTuple._2)
                .collect(Collectors.toList());
        this.offsets = cursors.stream().map(searcherManagerTermTuple -> 0).collect(Collectors.toList());
        this.finished = false;
        this.closed = false;
        this.sort = sort();
        this.afters = getAfters();
    }


    private void releaseSearchers() {
        indices.forEach(wrapc(i -> managers.get(i).release(searchers.get(i))));
    }

    /**
     * The sort of the query rewritten by the searcher.
     */
    private Sort sort() {
        try {
            return querySort.rewrite(searchers.iterator().next());
        } catch (Exception e) {
            releaseSearchers();
            throw new IndexException(e, "Error rewriting sort " + indexSort.toString());
        }
    }


    private List<Optional<ScoreDoc>> getAfters() {
        try {
            return indices.stream().map(i -> afterTerms.get(i).map(wrapf(term -> {
                //TODO: add time benchmark for debug
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                builder.add(new TermQuery(term), BooleanClause.Occur.FILTER);
                builder.add(query, BooleanClause.Occur.MUST);
                List<ScoreDoc> scores = Arrays.asList(searchers.get(i).search(builder.build(), 1, sort).scoreDocs);
                if (!scores.isEmpty()) {
                    return scores.iterator().next();
                } else {
                    throw new IndexException("");
                }
            }))).collect(Collectors.toList());

        } catch (Exception e) {
            releaseSearchers();
            throw new IndexException(e, "Error while searching for the last page position");
        }
    }

    private void fetch() throws Exception {

        try {
            //TODO:add benchmark for debug
            TopFieldDocs[] fieldDocs = indices.stream().map(wrapf(i -> {
                Optional<Term> afterTerm = afterTerms.get(i);
                if (!afterTerm.isPresent() && EarlyTerminatingSortingCollector.canEarlyTerminate(sort, indexSort)) {
                    FieldDoc fieldDoc = (FieldDoc) afters.get(i).orElse(null);
                    TopFieldCollector collector = null;
                    collector = TopFieldCollector.create(sort, pageSize, fieldDoc, true, false, false);
                    int hits = offsets.get(i) + pageSize;
                    EarlyTerminatingSortingCollector earlyCollector = new EarlyTerminatingSortingCollector(collector, sort, hits, indexSort);
                    searchers.get(i).search(query, earlyCollector);
                    TopFieldDocs topDocs = collector.topDocs();
                    offsets.set(i, offsets.get(i) + topDocs.scoreDocs.length);
                    return topDocs;
                } else {
                    return searchers.get(i).searchAfter(afters.get(i).orElse(null), query, pageSize, sort, false, false);
                }
            })).toArray(TopFieldDocs[]::new);

            // Merge partitions results
            ScoreDoc[] scoreDocs = TopDocs.merge(sort, pageSize, fieldDocs).scoreDocs;

            int numFetched = scoreDocs.length;

            finished = numFetched < pageSize;

            for (ScoreDoc scoreDoc : scoreDocs) {
                int shard = scoreDoc.shardIndex;
                afters.set(shard, Optional.of(scoreDoc));
                Document document = searchers.get(shard).doc(scoreDoc.doc, fields);
                documents.add(new Tuple<>(document, scoreDoc));
            }

        } catch (Exception e) {
            close();
            throw new IndexException(e, "Error searching with " + query.toString() + " and " + sort.toString());
        }
        if (finished) {
            close();
        }
    }

    public boolean needsFetch() {
        return !finished && documents.isEmpty();
    }


    @Override
    public void close() throws Exception {
        if (!closed) {
            try {
                releaseSearchers();
            } finally {
                closed = true;
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (needsFetch()) {
            try {
                fetch();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return !documents.isEmpty();
    }

    @Override
    public Tuple<Document, ScoreDoc> next() {
        if (hasNext()) {
            return documents.poll();
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public Iterator<Tuple<Document, ScoreDoc>> iterator() {
        return this;
    }

}
