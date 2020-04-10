package com.stratio.cassandra.lucene;

/** JMX methods to be exposed by {@link IndexService}.
 * @author Andres de la Pena `adelapena@stratio.com`
 **/
public interface IndexServiceMBean {

    /** Commits the pending changes. */
    void commit();


    /** Returns the total number of documents in this index.
     *
     * @return the number of documents
     */
    long getNumDocs();


    /** Returns the total number of deleted documents in this index.
     *
     * @return the number of deleted documents
     */
    long getNumDeletedDocs();


    /** Optimizes the index forcing merge segments leaving the specified number of segments. This
     * operation may block until all merging completes.
     *
     * @param maxNumSegments the maximum number of segments left in the index after merging finishes
     * @param doWait         `true` if the call should block until the operation completes
     */
    void forceMerge(int maxNumSegments, boolean doWait);


    /** Optimizes the index forcing merge of all segments that have deleted documents. This operation
     * may block until all merging completes.
     *
     * @param doWait `true` if the call should block until the operation completes
     */
    void forceMergeDeletes(boolean doWait);


    /** Refreshes the index readers. */
    void refresh();

}
