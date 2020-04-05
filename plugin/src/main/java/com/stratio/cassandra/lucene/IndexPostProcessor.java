package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.index.RAMIndex;
import com.stratio.cassandra.lucene.index.Tuple;
import com.stratio.cassandra.lucene.search.Search;
import com.stratio.cassandra.lucene.util.SimplePartitionIterator;
import com.stratio.cassandra.lucene.util.SingleRowIterator;
import com.stratio.cassandra.lucene.util.TimeCounter;
import com.stratio.cassandra.lucene.util.Tracer;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

/**Post processes in the coordinator node the results of a distributed search. In other words,
 * gets the k globally best results from all the k best node-local results.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 *
 **/
public abstract class IndexPostProcessor<A extends ReadQuery> implements BiFunction<PartitionIterator, A , PartitionIterator> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexPostProcessor.class);
    private static final Tracer TRACER = new Tracer();

    public static final String ID_FIELD = "_id";
    public static final Set<String> FIELDS_TO_LOAD = Collections.singleton(ID_FIELD);


    protected final IndexService service;


    /**
     * @param service the index service
     * */
    protected IndexPostProcessor(IndexService service) {
        this.service = service;
    }




    /** Returns a partition iterator containing the top-k rows of the specified partition iterator
     * according to the specified search.
     *
     * @param partitions a partition iterator
     * @param search     a search defining the ordering
     * @param limit      the number of results to be returned
     * @param now        the operation time in seconds
     * @return PartitionIterator
     */
    protected PartitionIterator process(PartitionIterator partitions,
                                        Search search,
                                        int limit,
                                        int now){
        if(search.requiresFullScan()){
            List<Tuple<DecoratedKey, SingleRowIterator>> rows = collect(partitions);
            if(search.requiresPostProcessing() && !rows.isEmpty()){
                return top(rows, search, limit, now);
            }
        }
        return partitions;
    }



    /** Collects the rows of the specified partition iterator. The iterator gets traversed after this
     * operation so it can't be reused.
     *
     * @param partitions a partition iterator
     * @return the rows contained in the partition iterator
     */
    private List<Tuple<DecoratedKey, SingleRowIterator>> collect(PartitionIterator partitions){
        TimeCounter.StartedTimeCounter time = TimeCounter.start();
        List<Tuple<DecoratedKey, SingleRowIterator>> rows = new ArrayList<>();
        partitions.forEachRemaining(iterator -> {
            try (RowIterator partition  = iterator){
                DecoratedKey key = partition.partitionKey();
                while (partition.hasNext()){
                    rows.add(new Tuple<>(key, new SingleRowIterator(partition)));
                }
            }
        });
        LOGGER.debug("Collected {} rows in {}", rows.size(), time.toString());
        return rows;
    }




    /** Takes the k best rows of the specified rows according to the specified search.
     *
     * @param rows   the rows to be sorted
     * @param search a search defining the ordering
     * @param limit  the number of results to be returned
     * @param now    the operation time in seconds
     * @return PartitionIterator
     */
    private PartitionIterator top(List<Tuple<DecoratedKey, SingleRowIterator>> rows,
                                  Search search,
                                  int limit,
                                  int now){

        TimeCounter.StartedTimeCounter time = TimeCounter.start();
        RAMIndex index  = new RAMIndex(service.schema().analyzer);
        try {
            for (int id = 0; id < rows.size(); id++) {
                Tuple<DecoratedKey, SingleRowIterator> tuple = rows.get(id);
                Row row = tuple._2.row;
                Document doc = document(tuple._1, row, search, now);
                doc.add(new StoredField(ID_FIELD, id));
                index.add(doc);
            }

            Query query = search.postProcessingQuery(service.schema());
            Sort sort = service.sort(search);
            List<Tuple<Document, ScoreDoc>> docs = index.search(query, sort, limit, FIELDS_TO_LOAD);

            List<SingleRowIterator> merged = new ArrayList<>();

            for (Tuple<Document, ScoreDoc> doc: docs) {
                int id = Integer.parseInt(doc._1.get(ID_FIELD));
                SingleRowIterator rowIterator = rows.get(id)._2;
                SingleRowIterator decorated = rowIterator.decorate(row -> service.expressionMapper().decorate( row, doc._2, now));
                merged.add(decorated);
            }

            TRACER.trace(MessageFormatter.format("Lucene post-process {} collected rows to {} rows", rows.size(), merged.size()).getMessage());
            LOGGER.debug("Post-processed {} rows to {} rows in {}", rows.size(), merged.size(), time.toString());
            return new SimplePartitionIterator(merged);
        }finally {
            index.close();
        }
    }

    /** Returns a [[Document]] representing the specified row with only the fields required to satisfy
     * the specified [[Search]].
     *
     * @param key    a partition key
     * @param row    a row
     * @param search a search
     * @return a document with just the fields required to satisfy the search
     */
    private Document document(DecoratedKey key, Row row, Search search, int now){
        Document document = new Document();
        Clustering clustering = row.clustering();
        Columns columns = service.columnsMapper().columns(key, row, now);
        service.keyIndexableFields(key, clustering).forEach(v1 -> document.add(v1));
        service.schema().postProcessingIndexableFields(columns, search).forEach(document::add);
        return document;
    }



    /** An [[IndexPostProcessor]] for [[ReadCommand]]s.
     *
     * @author Artem Martynenko artem7mag@gmai.com
     * @author Andres de la Pena `adelapena@stratio.com`
     */
    public static class ReadCommandPostProcessor extends IndexPostProcessor<ReadCommand>{

        /**
         * @param service the index service
         */
        protected ReadCommandPostProcessor(IndexService service) {
            super(service);
        }

        @Override
        public PartitionIterator apply(PartitionIterator partitionIterator, ReadCommand command) {
            if(!partitionIterator.hasNext() || command instanceof SinglePartitionReadCommand) return partitionIterator;
            Search search = service.expressionMapper().search(command);
            return process(partitionIterator, search, command.limits().count(), command.nowInSec());
        }
    }


    /** An [[IndexPostProcessor]] for [[Group]] commands.
     *
     * @author Artem Martynenko artem7mag@gmai.com
     * @author Andres de la Pena `adelapena@stratio.com`
     */
    public static class GroupPostProcessor extends IndexPostProcessor<SinglePartitionReadCommand.Group>{

        /**
         * @param service the index service
         */
        protected GroupPostProcessor(IndexService service) {
            super(service);
        }

        @Override
        public PartitionIterator apply(PartitionIterator partitionIterator, SinglePartitionReadCommand.Group group) {
            if(!partitionIterator.hasNext() || group.commands.size() <= 1) return partitionIterator;
            Search search = service.expressionMapper().search(group.commands.get(0));
            return process(partitionIterator, search, group.limits().count(), group.nowInSec());
        }
    }

}
