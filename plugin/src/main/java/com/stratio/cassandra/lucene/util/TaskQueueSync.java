package com.stratio.cassandra.lucene.util;

import java.io.IOException;
import java.util.function.Supplier;

/** Trivial {@link TaskQueue} not using parallel nor asynchronous processing
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class TaskQueueSync implements TaskQueue {


    @Override
    public void submitAsynchronous(Object id, Supplier<?> task) {
        task.get();
    }

    @Override
    public void submitSynchronous(Supplier<?> task) {
        task.get();
    }

    @Override
    public void close() throws IOException {

    }


}
