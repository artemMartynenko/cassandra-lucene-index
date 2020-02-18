package com.stratio.cassandra.lucene.util;

/**
 * @author Artem Martynenko artem7mag@gmai.com
 **/
@FunctionalInterface
public interface ThrowingConsumer<T, E extends Exception> {
    void accept(T t) throws E;
}
