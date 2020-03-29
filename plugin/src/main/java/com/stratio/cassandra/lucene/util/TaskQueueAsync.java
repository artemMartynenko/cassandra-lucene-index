package com.stratio.cassandra.lucene.util;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class TaskQueueAsync implements TaskQueue{
    @Override
    public void submitAsynchronous(Object id, Supplier<?> task) {

    }

    @Override
    public void submitSynchronous(Supplier<?> task) {

    }

    @Override
    public void close() throws IOException {

    }
}
