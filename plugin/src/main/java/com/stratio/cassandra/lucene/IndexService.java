package com.stratio.cassandra.lucene;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.index.DocumentIterator;
import com.stratio.cassandra.lucene.index.PartitionedIndex;
import com.stratio.cassandra.lucene.index.Tuple;
import com.stratio.cassandra.lucene.mapping.ColumnsMapper;
import com.stratio.cassandra.lucene.mapping.ExpressionMapper;
import com.stratio.cassandra.lucene.mapping.PartitionMapper;
import com.stratio.cassandra.lucene.mapping.TokenMapper;
import com.stratio.cassandra.lucene.partitioning.Partitioner;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.search.Search;
import com.stratio.cassandra.lucene.util.Locker;
import com.stratio.cassandra.lucene.util.TaskQueue;
import com.stratio.cassandra.lucene.util.TaskQueueBuilder;
import com.stratio.cassandra.lucene.util.Tracer;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import javax.management.JMException;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Lucene index service provider.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 **/
public abstract class IndexService implements IndexServiceMBean{

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexService.class);
    private static final Tracer TRACER = new Tracer();

    protected final ColumnFamilyStore table;
    protected final IndexMetadata indexMetadata;


    public final CFMetaData metaData;
    public final String ksName;
    public final String cfName;
    public final String idxName;
    public final String qualifiedName;

    public final IndexOptions options;



    public final Schema schema;
    public final Set<ColumnDefinition> regulars;
    public final Set<String> mappedRegulars;
    public final boolean mapsMultiCell;
    public final boolean mapsPrimaryKey;

    public final boolean excludedDataCenter;


    public final TokenMapper tokenMapper;
    public final PartitionMapper partitionMapper;
    public final ColumnsMapper columnsMapper;
    public final ExpressionMapper expressionMapper;


    public final TaskQueue queue;
    public final Partitioner partitioner;

    public final PartitionedIndex lucene;

    protected ObjectName mBean;

    public final Locker readBeforeWriteLocker;


    /**
     * @param table         the indexed table
     * @param indexMetadata the index metadata
     * */
    public IndexService(ColumnFamilyStore table, IndexMetadata indexMetadata) {
        this.table = table;
        this.indexMetadata = indexMetadata;

        metaData = table.metadata;
        ksName = metaData.ksName;
        cfName = metaData.cfName;
        idxName = indexMetadata.name;
        qualifiedName = String.join(".",ksName, cfName, idxName);

        // Parse options
        options = new IndexOptions(metaData, indexMetadata);

        // Setup schema
        schema = options.schema;
        regulars = Sets.newHashSet(metaData.partitionColumns().regulars.complexColumns());
        mappedRegulars = regulars.stream().map(columnDefinition -> columnDefinition.name.toString())
                .filter(s -> schema.mappedCells().contains(s))
                .collect(Collectors.toSet());

        mapsMultiCell = regulars.stream()
                .anyMatch(columnDefinition -> columnDefinition.type.isMultiCell()
                        &&
                        schema.mapsCell(columnDefinition.name.toString()));

        mapsPrimaryKey = Lists.newArrayList(metaData.primaryKeyColumns().iterator())
                .stream()
                .anyMatch(columnDefinition -> schema.mapsCell(columnDefinition.name.toString()));

        excludedDataCenter = options.excludedDataCenters.contains(DatabaseDescriptor.getLocalDataCenter());


        // Setup mapping
        tokenMapper = new TokenMapper();
        partitionMapper = new PartitionMapper(metaData);
        columnsMapper = new ColumnsMapper(schema, metaData);
        expressionMapper = new ExpressionMapper(metaData, indexMetadata);

        // Setup FS index and write queue
        queue = TaskQueueBuilder.build(options.indexingThreads, options.indexingQueuesSize);
        partitioner = options.partitioner;

        lucene = new PartitionedIndex(partitioner.numPartitions(),
                idxName,
                options.path,
                options.schema.analyzer,
                options.refreshSeconds,
                options.ramBufferMB,
                options.maxMergeMB,
                options.maxCachedMB);

        // Setup indexing read-before-write lock provider
        readBeforeWriteLocker = new Locker(DatabaseDescriptor.getConcurrentWriters() * 128);

    }

    public void init(){

        // Initialize index
        try {
            Sort sort = new Sort(keySortFields().toArray(new SortField[0]));
            if(!excludedDataCenter){
                lucene.init(sort, fieldsToLoad());
            }
        }catch (Exception e){
            LOGGER.error(MessageFormatter.format("Initialization of Lucene FS directory for index {} has failed", idxName).getMessage(), e);
        }

        // Register JMX MBean
        try {
            String mBeanName = "com.stratio.cassandra.lucene:type=Lucene," + "keyspace="+ksName + ",table="+ table + ",index="+idxName;
            mBean = new ObjectName(mBeanName);
        }catch (JMException e){
            LOGGER.error("Error while registering Lucene index JMX MBean", e);
        }

    }


    /** Returns the Lucene {@link SortField}s required to retrieve documents sorted by primary key.
     *
     * @return the sort fields
     */
    public abstract List<SortField> keySortFields();

    /** Returns the names of the Lucene fields to be loaded from index during searches.
     *
     * @return the names of the fields to be loaded
     */
    public abstract Set<String> fieldsToLoad();


    public abstract List<IndexableField> keyIndexableFields(DecoratedKey key, Clustering clustering);




    /** Returns if the specified column definition is mapped by this index.
     *
     * @param columnDef a column definition
     * @return `true` if the column is mapped, `false` otherwise
     */
    public boolean dependsOn(ColumnDefinition columnDef){
        return schema.mapsCell(columnDef.name.toString());
    }



    /** Returns the validated search contained in the specified expression.
     *
     * @param expression a custom CQL expression
     * @return the validated expression
     */
    public Search validate(RowFilter.Expression expression){
       return expressionMapper.search(expression).validate(schema);
    }


    /** Returns a Lucene term uniquely identifying the specified row.
     *
     * @param key        the partition key
     * @param clustering the clustering key
     * @return a Lucene identifying term
     */
    public abstract Term term(DecoratedKey key, Clustering clustering);


    /** Returns a Lucene term identifying documents representing all the row's which are in the
     * partition the specified {@link DecoratedKey}.
     *
     * @param key the partition key
     * @return a Lucene term representing `key`
     */
    public Term term(DecoratedKey key){
        return  partitionMapper.term(key);
    }

    /** Returns if SSTables can contain additional columns of the specified row so read-before-write
     * is required prior to indexing.
     *
     * @param key the partition key
     * @param row the row
     * @return `true` if read-before-write is required, `false` otherwise
     */
    public boolean needsReadBeforeWrite(DecoratedKey key, Row row){
        return mapsMultiCell || !row.columns()
                .stream()
                .map(columnDefinition -> columnDefinition.name.toString())
                .collect(Collectors.toList())
                .containsAll(mappedRegulars);
    }




    /**
     * Returns true if the supplied Row contains at least one column that is present
     * in the index schema.
     *
     * @param row the row
     * @return `true` if the index must be updated, `false` otherwise
     */
    public boolean doesAffectIndex(Row row){
        return !options.sparse || mapsPrimaryKey || row.columns().stream()
                .anyMatch(columnDefinition -> mappedRegulars.contains(columnDefinition.name.toString()));
    }



    /** Returns the {@link DecoratedKey} contained in the specified Lucene document.
     *
     * @param document the document containing the partition key to be get
     * @return the partition key contained in the specified Lucene document
     */
    public DecoratedKey decoratedKey(Document document){
        return  partitionMapper.decoratedKey(document);
    }


    /** Creates an new {@link IndexWriter} object for updates to a given partition.
     *
     * @param key             key of the partition being modified
     * @param nowInSec        current time of the update operation
     * @param orderGroup      operation group spanning the update operation
     * @param transactionType what kind of update is being performed on the base data
     * @return the newly created index writer
     */
    public abstract IndexWriter writer(DecoratedKey key,
                              int nowInSec,
                              OpOrder.Group orderGroup,
                              IndexTransaction.Type transactionType);


    /** Deletes all the index contents. */
    public void truncate(){
        if(!excludedDataCenter){
            queue.submitSynchronous(() -> {lucene.truncate(); return lucene;});
        }
    }


    /** Closes and removes all the index files. */
    public void delete(){
        try {
            if(!excludedDataCenter) {
                queue.close();
            }
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(mBean);
        }catch (JMException e){
            LOGGER.error("Error while unregistering Lucene index MBean", e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(!excludedDataCenter){
                lucene.delete();
            }
        }
    }



    /** Upserts the specified row.
     *
     * @param key      the partition key
     * @param row      the row
     * @param nowInSec now in seconds
     */
    public void upsert(DecoratedKey key, Row row, int nowInSec){
        if(!excludedDataCenter){
            queue.submitAsynchronous(key, () -> {
                int partition = partitioner.partition(key);
                Clustering clustering = row.clustering();
                Term term = this.term(key, clustering);
                Columns columns = columnsMapper.columns(key, row, nowInSec);
                List<IndexableField> fileds = schema.indexableFields(columns);
                if(fileds.isEmpty()){
                    lucene.delete(partition, term);
                }else {
                    Document doc = new Document();
                    keyIndexableFields(key, clustering).forEach(doc::add);
                    fileds.forEach(doc::add);
                    lucene.upsert(partition, term, doc);
                }
                return lucene;
            });
        }
    }


    /** Deletes the partition identified by the specified key.
     *
     * @param key        the partition key
     * @param clustering the clustering key
     */
    public void delete(DecoratedKey key, Clustering clustering){
        if(!excludedDataCenter){
            queue.submitAsynchronous(key, () -> {
                int partition = partitioner.partition(key);
                Term term = this.term(key, clustering);
                lucene.delete(partition, term);
                return lucene;
            });
        }
    }


    /** Deletes the partition identified by the specified key.
     *
     * @param key the partition key
     */
    public void delete(DecoratedKey key){
        if(!excludedDataCenter){
            queue.submitAsynchronous(key, () -> {
                int partition = partitioner.partition(key);
                Term term = this.term(key);
                lucene.delete(partition, term);
                return lucene;
            });
        }
    }



    public UnfilteredPartitionIterator search(ReadCommand command,
                                              ReadExecutionController controller){
        if(!excludedDataCenter){
            TRACER.trace("Building Lucene search");
            Search search = expressionMapper.search(command);
            Query query = search.query(schema, query(command).orElse(null));
            List<Tuple<Integer,Optional<Term>>> afters = after(search.paging(), command);
            Sort sort = sort(search);
            int count = command.limits().count();


            if(search.refresh()){
                TRACER.trace("Refreshing Lucene index searcher");
                refresh();
            }

            TRACER.trace(MessageFormatter.format("Lucene index searching for {} rows", count).getMessage());
            List<Integer> partitions = partitioner.partitions(command);
            List<Tuple<Integer,Optional<Term>>> readers = afters.stream()
                    .filter(integerOptionalTuple -> partitions.contains(integerOptionalTuple._1))
                    .map(integerOptionalTuple -> new Tuple<>(integerOptionalTuple._1, integerOptionalTuple._2))
                    .collect(Collectors.toList());
            DocumentIterator documents = lucene.search(readers, query, sort, count); //TODO: rethought about Optional<Term> and Term
            return reader(documents, command, controller);
        }else {
           return new IndexReaderExcludingDataCenter(command, table);
        }
    }


    /** Returns the key range query represented by the specified read command.
     *
     * @param command the read command } else {
     * @return the key range query
     */
    public Optional<Query> query(ReadCommand command){
        if(command instanceof SinglePartitionReadCommand){
            DecoratedKey key = ((SinglePartitionReadCommand) command).partitionKey();
            ClusteringIndexFilter filter = command.clusteringIndexFilter(key);
            return Optional.ofNullable(query(key, filter));
        } else if(command instanceof PartitionRangeReadCommand){
            return query(((PartitionRangeReadCommand) command).dataRange());
        } else {
            throw new IndexException("Unsupported read command {}", command.getClass());
        }
    }


    /** Returns a query to get the documents satisfying the specified key and clustering filter.
     *
     * @param key    the partition key
     * @param filter the clustering key range
     * @return a query to get the documents satisfying the key range
     */
    public abstract Query query(DecoratedKey key, ClusteringIndexFilter filter);

    /** Returns a query to get the documents satisfying the specified data range.
     *
     * @param dataRange the data range
     * @return a query to get the documents satisfying the data range
     */
    public abstract Optional<Query> query(DataRange dataRange);


    public List<Tuple<Integer,Optional<Term>>> after(IndexPagingState pagingState, ReadCommand command){
        List<Integer> partitions = partitioner.partitions(command);
        List<Optional<Term>> afters ;
        if(pagingState == null){
           afters =  IntStream.range(0, partitioner.numPartitions())
                   .mapToObj(value -> Optional.<Term>empty())
                   .collect(Collectors.toList());
        } else {
            afters = pagingState.forCommand(command, partitioner).stream()
                    .map(tuple -> tuple.map(innerTuple -> after(innerTuple._1._2, innerTuple._2)))
                    .collect(Collectors.toList());
        }
        return partitions.stream()
                .map(i ->  new Tuple<>(i, afters.get(i)))
                .collect(Collectors.toList());
    }


    /** Returns a Lucene query to retrieve the row identified by the specified paging state.
     *
     * @param key        the partition key
     * @param clustering the clustering key
     * @return the query to retrieve the row
     */
    public abstract Term after(DecoratedKey key, Clustering clustering);


    /** Returns the Lucene sort with the specified search sorting requirements followed by the
     * Cassandra's natural ordering based on partitioning token and cell name.
     *
     * @param search the search containing sorting requirements
     * @return a Lucene sort according to `search`
     */
    public Sort sort(Search search){
        List<SortField> sortFields = new ArrayList<>();
        if(search.usesSorting()){
            sortFields.addAll(search.sortFields(schema));
        }
        if(search.usesRelevance()){
            sortFields.add(SortField.FIELD_SCORE);
        }

        sortFields.addAll(keySortFields());

        return new Sort(sortFields.toArray(new SortField[0]));
    }


    /** Reads from the local SSTables the rows identified by the specified search.
     *
     * @param documents  the Lucene documents
     * @param command    the Cassandra command
     * @param controller the read execution controller
     * @return the local rows satisfying the search
     */
    public abstract IndexReader reader(DocumentIterator documents,
                                       ReadCommand command,
                                       ReadExecutionController controller);


    /** Ensures that values present in a partition update are valid according to the schema.
     *
     * @param update the partition update containing the values to be validated
     */
    public void validate(PartitionUpdate update){
        DecoratedKey key = update.partitionKey();
        int now = FBUtilities.nowInSeconds();
        update.forEach(row -> schema.validate(columnsMapper.columns(key, row , now)));
    }


    @Override
    public void commit() {
        if(!excludedDataCenter){
            queue.submitSynchronous(() -> {
                lucene.commit();
                return lucene;
            });
        }
    }

    @Override
    public long getNumDocs() {
        return !excludedDataCenter ? lucene.getNumDocs() : 0;
    }

    @Override
    public long getNumDeletedDocs() {
        return !excludedDataCenter ? lucene.getNumDeletedDocs() : 0;
    }

    @Override
    public void forceMerge(int maxNumSegments, boolean doWait) {
        if(!excludedDataCenter){
            queue.submitSynchronous(() -> {
                        lucene.forceMerge(maxNumSegments, doWait);
                        return lucene;
                }
            );
        }
    }

    @Override
    public void forceMergeDeletes(boolean doWait) {
        if(!excludedDataCenter){
            queue.submitSynchronous(() -> {
                        lucene.forceMergeDeletes(doWait);
                        return lucene;
                    }
            );
        }
    }

    @Override
    public void refresh() {
        if(!excludedDataCenter){
            queue.submitSynchronous(() -> {
                lucene.refresh();
                return lucene;
            });
        }
    }
}
