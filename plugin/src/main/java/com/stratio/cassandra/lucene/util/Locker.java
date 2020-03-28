package com.stratio.cassandra.lucene.util;

import com.google.common.base.Preconditions;

import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class Locker {


    private final int numLocks;
    private final Object[] locks;

    public Locker(int numLocks) {
        Preconditions.checkArgument(numLocks > 0,  "The number of concurrent locks should be strictly positive but found %s",numLocks);
        this.numLocks = numLocks;
        this.locks = IntStream.rangeClosed(1, numLocks).boxed().map(integer -> new Object()).toArray();
    }

    public void run(Object id, Supplier<?> task){
        synchronized (locks[Math.abs(id.hashCode() % numLocks)]) {
            task.get();
        }
    }

}
