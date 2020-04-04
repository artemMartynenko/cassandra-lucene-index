package com.stratio.cassandra.lucene.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**A queue that executes each submitted task using one of possibly several pooled threads.
 * Tasks can be submitted with an identifier, ensuring that all tasks with same identifier will be
 * executed orderly in the same thread. Each thread has its own task queue.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public interface TaskQueue extends Closeable {


    /** Submits a non value-returning task for asynchronous execution.
     *
     * The specified identifier is used to choose the thread executor where the task will be queued.
     * The selection and load balancing is based in the hashcode of the supplied id.
     *
     * @param id   the identifier of the task used to choose the thread executor where the task will
     *             be queued for asynchronous execution
     * @param task the task to be queued for asynchronous execution
     */
    void submitAsynchronous(Object id, Supplier<?> task);



    /** Submits a non value-returning task for synchronous execution. It waits for all synchronous
     * tasks to be completed.
     *
     * @param task a task to be executed synchronously
     * @return the result of the task
     */
    void submitSynchronous(Supplier<?> task);

}
