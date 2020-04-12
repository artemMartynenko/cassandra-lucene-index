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
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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


    protected final CFMetaData metaData;
    protected final String ksName;
    protected final String cfName;
    protected final String idxName;
    protected final String qualifiedName;

    protected final IndexOptions options;



    protected final Schema schema;
    protected final Set<ColumnDefinition> regulars;
    protected final Set<String> mappedRegulars;
    protected final boolean mapsMultiCell;
    protected final boolean mapsPromaryKey;

    protected final boolean excludedDataCentr;


    protected final TokenMapper tokenMapper;
    protected final PartitionMapper partitionMapper;
    protected final ColumnsMapper columnsMapper;
    protected final ExpressionMapper expressionMapper;


    protected final TaskQueue queue;
    protected final Partitioner partitioner;

    protected final PartitionedIndex lucene;

    protected ObjectName mBean;

    protected final Locker readBeforeWriteLocker;


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

        mapsPromaryKey = Lists.newArrayList(metaData.primaryKeyColumns().iterator())
                .stream()
                .anyMatch(columnDefinition -> schema.mapsCell(columnDefinition.name.toString()));

        excludedDataCentr = options.excludedDataCenters.contains(DatabaseDescriptor.getLocalDataCenter());


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
            if(!excludedDataCentr){
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
    abstract List<SortField> keySortFields();

    /** Returns the names of the Lucene fields to be loaded from index during searches.
     *
     * @return the names of the fields to be loaded
     */
    abstract Set<String> fieldsToLoad();


    abstract List<IndexableField> keyIndexableFields(DecoratedKey key, Clustering clustering);




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
        return !options.sparse || mapsPromaryKey || row.columns().stream()
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
        if(!excludedDataCentr){
            queue.submitSynchronous(() -> {lucene.truncate(); return lucene;});
        }
    }


    /** Closes and removes all the index files. */
    public void delete(){
        try {
            if(!excludedDataCentr) {
                queue.close();
            }
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(mBean);
        }catch (JMException e){
            LOGGER.error("Error while unregistering Lucene index MBean", e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(!excludedDataCentr){
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
        if(!excludedDataCentr){
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
        if(!excludedDataCentr){
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
        if(!excludedDataCentr){
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
        if(!excludedDataCentr){
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
                    .collect(Collectors.toList());
            DocumentIterator documents = lucene.search()//TODO: continue here

        }else {
           return new IndexReaderExcludingDataCenter(command, table);
        }
    }


    public Optional<Query> query(ReadCommand command){

    }


    public List<Tuple<Integer,Optional<Term>>> after(IndexPagingState pagingState, ReadCommand command){

    }



    public Sort sort(Search search){

    }


    @Override
    public void commit() {

    }

    @Override
    public long getNumDocs() {
        return 0;
    }

    @Override
    public long getNumDeletedDocs() {
        return 0;
    }

    @Override
    public void forceMerge(int maxNumSegments, boolean doWait) {

    }

    @Override
    public void forceMergeDeletes(boolean doWait) {

    }

    @Override
    public void refresh() {

    }
}
