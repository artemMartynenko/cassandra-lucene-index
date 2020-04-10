package com.stratio.cassandra.lucene;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.index.PartitionedIndex;
import com.stratio.cassandra.lucene.mapping.ColumnsMapper;
import com.stratio.cassandra.lucene.mapping.ExpressionMapper;
import com.stratio.cassandra.lucene.mapping.PartitionMapper;
import com.stratio.cassandra.lucene.mapping.TokenMapper;
import com.stratio.cassandra.lucene.partitioning.Partitioner;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.util.Locker;
import com.stratio.cassandra.lucene.util.TaskQueue;
import com.stratio.cassandra.lucene.util.TaskQueueBuilder;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import javax.management.JMException;
import javax.management.ObjectName;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Lucene index service provider.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 **/
public abstract class IndexService {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexService.class);

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


    /** Returns the Lucene [[SortField]]s required to retrieve documents sorted by primary key.
     *
     * @return the sort fields
     */
    abstract List<SortField> keySortFields();

    /** Returns the names of the Lucene fields to be loaded from index during searches.
     *
     * @return the names of the fields to be loaded
     */
    abstract Set<String> fieldsToLoad();



}
