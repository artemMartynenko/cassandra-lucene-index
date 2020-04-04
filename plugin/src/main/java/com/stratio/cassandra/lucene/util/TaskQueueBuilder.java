package com.stratio.cassandra.lucene.util;

/**
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class TaskQueueBuilder {


    /** Returns a new {@link TaskQueue}.
     *
     * @param numThreads the number of executor threads
     * @param queuesSize the max number of tasks in each thread queue before blocking
     * @return a new task queue
     */
    public static TaskQueue build(int numThreads, int queuesSize){
        return numThreads > 0 ? new TaskQueueAsync(numThreads, queuesSize) : new TaskQueueSync();
    }
}
