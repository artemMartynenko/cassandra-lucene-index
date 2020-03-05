package com.stratio.cassandra.lucene.index;

import com.google.common.collect.Lists;
import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Sort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * An [[FSIndex]] partitioned by some not specified criterion.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 */
public class PartitionedIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedIndex.class);

    private Sort mergeSort;
    private Set<String> fields;
    private List<FSIndex> indexes;

    private int partitions;
    private String name;
    private Path path;
    private Analyzer analyzer;
    private Double refreshSeconds;
    private Integer ramBufferMB;
    private Integer maxMergeMB;
    private Integer maxCachedMB;


    /**
     * @param partitions     the number of index partitions
     * @param name           the index name
     * @param path           the directory path
     * @param analyzer       the index writer analyzer
     * @param refreshSeconds the index reader refresh frequency in seconds
     * @param ramBufferMB    the index writer RAM buffer size in MB
     * @param maxMergeMB     the directory max merge size in MB
     * @param maxCachedMB    the directory max cache size in MB
     */
    public PartitionedIndex(int partitions, String name, Path path, Analyzer analyzer, Double refreshSeconds, Integer ramBufferMB, Integer maxMergeMB, Integer maxCachedMB) {
        this.partitions = partitions;
        this.name = name;
        this.path = path;
        this.analyzer = analyzer;
        this.refreshSeconds = refreshSeconds;
        this.ramBufferMB = ramBufferMB;
        this.maxMergeMB = maxMergeMB;
        this.maxCachedMB = maxCachedMB;
    }


    private List<FSIndex> indexes() {
        if (partitions == 1) {
            return Lists.newArrayList(new FSIndex(name, path, analyzer, refreshSeconds, ramBufferMB, maxMergeMB, maxCachedMB));
        } else if (partitions > 1) {
            String root = path.toFile().getAbsolutePath() + File.separator;
            return IntStream.range(0, partitions)
                    .boxed()
                    .map(i -> root + File.separator + i)
                    .map(Paths::get)
                    .map(path1 -> new FSIndex(name, path1, analyzer, refreshSeconds, ramBufferMB, maxMergeMB, maxCachedMB))
                    .collect(Collectors.toList());
        } else {
            throw new IndexException("The number of partitions should be strictly positive but found " + partitions);
        }
    }


    /**
     * Initializes this index with the specified merge sort and fields to be loaded.
     *
     * @param mergeSort the sort to be applied to the index during merges
     * @param fields    the names of the document fields to be loaded
     */
    public void init(Sort mergeSort, Set<String> fields) {
        this.mergeSort = mergeSort;
        this.fields = fields;
        this.indexes = indexes();
        indexes.forEach(fsIndex -> fsIndex.init(mergeSort, fields));
    }


    /**
     * Deletes all the documents.
     */
    public void truncate() {
        indexes.forEach(FSIndex::truncate);
        LOGGER.info("Truncated {}", name);
    }

    /**
     * Commits the pending changes.
     */
    public void commit() {
        indexes.forEach(FSIndex::commit);
        LOGGER.info("Committed {}", name);
    }


    /**
     * Commits all changes to the index, waits for pending merges to complete, and closes all
     * associated resources.
     */
    public void close() {
        indexes.forEach(FSIndex::close);
        LOGGER.info("Closed {}", name);
    }

    /**
     * Closes the index and removes all its files.
     */
    public void delete() {
        try {
            indexes.forEach(FSIndex::delete);
        } finally {
            if (partitions > 1) {
                FileUtils.deleteRecursive(path.toFile());
            }
        }
        LOGGER.info("Deleted {}", name);
    }

    /**
     * Optimizes the index forcing merge segments leaving the specified number of segments.
     * This operation may block until all merging completes.
     *
     * @param maxNumSegments the maximum number of segments left in the index after merging finishes
     * @param doWait         `true` if the call should block until the operation completes
     */
    public void forceMerge(int maxNumSegments, boolean doWait) {
        LOGGER.info("Merging {} segments to {}", name, maxNumSegments);
        indexes.forEach(fsIndex -> fsIndex.forceMerge(maxNumSegments, doWait));
        LOGGER.info("Merged {} segments to {}", name, maxNumSegments);
    }


    /**
     * Refreshes the index readers.
     */
    public void refresh() {
        indexes.forEach(FSIndex::refresh);
        LOGGER.debug("Refreshed {} readers", name);
    }


    /** Returns the total number of deleted documents in this index.
     *
     * @return the number of deleted documents
     */
    public long getNumDocs() {
        LOGGER.debug("Getting {} num deleted docs", name);
        return indexes.stream().mapToInt(FSIndex::getNumDeletedDocs)
                .sum();
    }


}
