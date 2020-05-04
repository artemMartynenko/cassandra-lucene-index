package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.partitioning.Partitioner;
import com.stratio.cassandra.lucene.partitioning.PartitionerOnNone;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.SchemaBuilder;
import com.stratio.cassandra.lucene.util.SchemaValidator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.schema.IndexMetadata;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Index user-specified configuration options parser.
 *
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class IndexOptions {
    public static final String REFRESH_SECONDS_OPTION = "refresh_seconds";
    public static final double DEFAULT_REFRESH_SECONDS = 60;

    public static final String RAM_BUFFER_MB_OPTION = "ram_buffer_mb";
    public static final int DEFAULT_RAM_BUFFER_MB = 64;

    public static final String MAX_MERGE_MB_OPTION = "max_merge_mb";
    public static final int DEFAULT_MAX_MERGE_MB = 5;

    public static final String MAX_CACHED_MB_OPTION = "max_cached_mb";
    public static final int DEFAULT_MAX_CACHED_MB = 30;

    public static final String INDEXING_THREADS_OPTION = "indexing_threads";
    public static final int DEFAULT_INDEXING_THREADS = Runtime.getRuntime().availableProcessors();

    public static final String INDEXING_QUEUES_SIZE_OPTION = "indexing_queues_size";
    public static final int DEFAULT_INDEXING_QUEUES_SIZE = 50;

    public static final String EXCLUDED_DATA_CENTERS_OPTION = "excluded_data_centers";
    public static final List<String> DEFAULT_EXCLUDED_DATA_CENTERS = Collections.emptyList();

    public static final String DIRECTORY_PATH_OPTION = "directory_path";
    public static final String INDEXES_DIR_NAME = "lucene";

    public static final String SCHEMA_OPTION = "schema";

    public static final String PARTITIONER_OPTION = "partitioner";
    public static final Partitioner DEFAULT_PARTITIONER = new PartitionerOnNone();

    public static final String SPARSE_OPTION = "sparse";
    public static final boolean DEFAULT_SPARSE = false;
    public final Map<String, String> options;
    /**
     * The Lucene index searcher refresh frequency, in seconds
     */
    public final double refreshSeconds;
    /**
     * The Lucene's max RAM buffer size, in MB
     */
    public final int ramBufferMB;
    /**
     * The Lucene's max segments merge size size, in MB
     */
    public final int maxMergeMB;
    /**
     * The Lucene's max cache size, in MB
     */
    public final int maxCachedMB;
    /**
     * The number of asynchronous indexing threads
     */
    public final int indexingThreads;
    /**
     * The size of the asynchronous indexing queues
     */
    public final int indexingQueuesSize;
    /**
     * The names of the data centers excluded from indexing
     */
    public final List<String> excludedDataCenters;
    /**
     * The mapping schema
     */
    public final Schema schema;
    /**
     * The index partitioner
     */
    public final Partitioner partitioner;
    /**
     * The path of the directory where the index files will be stored
     */
    public final Path path;
    /**
     * If the index is sparse or not
     */
    public final boolean sparse;

    /**
     * @param tableMetadata the indexed table metadata
     * @param indexMetadata the index metadata
     */
    public IndexOptions(CFMetaData tableMetadata, IndexMetadata indexMetadata) {
        this.options = indexMetadata.options;
        this.refreshSeconds = parseRefresh(options);
        this.ramBufferMB = parseRamBufferMB(options);
        this.maxMergeMB = parseMaxMergeMB(options);
        this.maxCachedMB = parseMaxCachedMB(options);
        this.indexingThreads = parseIndexingThreads(options);
        this.indexingQueuesSize = parseIndexingQueuesSize(options);
        this.excludedDataCenters = parseExcludedDataCenters(options);
        this.schema = parseSchema(options, tableMetadata);
        this.partitioner = parsePartitioner(options, tableMetadata);
        this.path = parsePath(options, tableMetadata, indexMetadata);
        this.sparse = parseSparse(options);
    }

    /**
     * Validates the specified index options.
     *
     * @param options  the options to be validated
     * @param metaData the indexed table metadata
     */
    public static void validate(Map<String, String> options, CFMetaData metaData) {
        parseRefresh(options);
        parseRamBufferMB(options);
        parseMaxMergeMB(options);
        parseMaxCachedMB(options);
        parseIndexingThreads(options);
        parseIndexingQueuesSize(options);
        parseExcludedDataCenters(options);
        parseSchema(options, metaData);
        parsePath(options, metaData, null);
        parsePartitioner(options, metaData);
    }

    public static double parseRefresh(Map<String, String> options) {
        return parseStrictlyPositiveDouble(options, REFRESH_SECONDS_OPTION, DEFAULT_REFRESH_SECONDS);
    }

    public static int parseRamBufferMB(Map<String, String> options) {
        return parseStrictlyPositiveInt(options, RAM_BUFFER_MB_OPTION, DEFAULT_RAM_BUFFER_MB);
    }

    public static int parseMaxMergeMB(Map<String, String> options) {
        return parseStrictlyPositiveInt(options, MAX_MERGE_MB_OPTION, DEFAULT_MAX_MERGE_MB);
    }

    public static int parseMaxCachedMB(Map<String, String> options) {
        return parseStrictlyPositiveInt(options, MAX_CACHED_MB_OPTION, DEFAULT_MAX_CACHED_MB);
    }

    public static int parseIndexingThreads(Map<String, String> options) {
        return parseInt(options, INDEXING_THREADS_OPTION, DEFAULT_INDEXING_THREADS);
    }

    public static int parseIndexingQueuesSize(Map<String, String> options) {
        return parseStrictlyPositiveInt(options, INDEXING_QUEUES_SIZE_OPTION, DEFAULT_INDEXING_QUEUES_SIZE);
    }

    public static List<String> parseExcludedDataCenters(Map<String, String> options) {
        return Optional.ofNullable(options.get(EXCLUDED_DATA_CENTERS_OPTION))
                .map(s -> Arrays.stream(s.split(","))
                        .map(String::trim)
                        .filter(s1 -> !s1.isEmpty())
                        .collect(Collectors.toList()))
                .orElse(DEFAULT_EXCLUDED_DATA_CENTERS);
    }

    public static Path parsePath(Map<String, String> options, CFMetaData table, IndexMetadata index) {
        return Optional.ofNullable(options.get(DIRECTORY_PATH_OPTION))
                .map(Paths::get).orElseGet(() -> Optional.ofNullable(index).map(indexMetadata -> {
                    Directories directories = new Directories(table);
                    String basePath = directories.getDirectoryForNewSSTables().getAbsolutePath();
                    return Paths.get(basePath + File.separator + INDEXES_DIR_NAME + File.separator + indexMetadata.name);
                }).orElse(null));
    }

    public static Schema parseSchema(Map<String, String> options, CFMetaData table) {
        return Optional.ofNullable(options.get(SCHEMA_OPTION))
                .map(value -> {
                    try {
                        Schema schema = SchemaBuilder.fromJson(value).build();
                        SchemaValidator.validate(schema, table);
                        return schema;
                    } catch (Exception e) {
                        throw new IndexException("{} is invalid : {}", SCHEMA_OPTION, e.getMessage());
                    }
                }).orElseThrow(() -> new IndexException(" {} is required", SCHEMA_OPTION));
    }

    public static Partitioner parsePartitioner(Map<String, String> options, CFMetaData table) {
        return Optional.ofNullable(options.get(PARTITIONER_OPTION))
                .map(value -> {
                    try {
                        return Partitioner.fromJson(table, value);
                    } catch (Exception e) {
                        throw new IndexException("{} is invalid : {}", PARTITIONER_OPTION, e.getMessage());
                    }
                }).orElse(DEFAULT_PARTITIONER);
    }

    public static boolean parseSparse(Map<String, String> options) {
        return Optional.ofNullable(options.get(SPARSE_OPTION))
                .map(Boolean::parseBoolean).orElse(DEFAULT_SPARSE);
    }

    public static int parseInt(Map<String, String> options,
                               String name,
                               int defaultValue) {
        return Optional.ofNullable(options.get(name)).map(s -> {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                throw new IndexException("'{}' must be an integer, found: {}", name, s);
            }
        }).orElse(defaultValue);
    }

    public static int parseStrictlyPositiveInt(Map<String, String> options,
                                               String name,
                                               int defaultValue) {

        return Optional.ofNullable(options.get(name)).map(s -> {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                throw new IndexException("'{}' must be a strictly positive integer, found: {}", name, s);
            }
        }).map(integer -> {
            if (integer > 0) {
                return integer;
            } else {
                throw new IndexException("'{}' must be a strictly positive, found: {}", name, integer);
            }
        }).orElse(defaultValue);
    }

    public static double parseStrictlyPositiveDouble(Map<String, String> options,
                                                     String name,
                                                     double defaultValue) {

        return Optional.ofNullable(options.get(name)).map(s -> {
            try {
                return Double.parseDouble(s);
            } catch (NumberFormatException e) {
                throw new IndexException("'{}' must be a strictly positive decimal, found: {}", name, s);
            }
        }).map(d -> {
            if (d > 0) {
                return d;
            } else {
                throw new IndexException("'{}' must be a strictly positive, found: {}", name, d);
            }
        }).orElse(defaultValue);
    }
}
