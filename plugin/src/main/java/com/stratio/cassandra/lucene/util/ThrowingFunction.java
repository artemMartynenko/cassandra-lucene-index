package com.stratio.cassandra.lucene.util;

import java.util.function.Function;

/**
 * @author Artem Martynenko artem7mag@gmai.com
 **/
@FunctionalInterface
public interface ThrowingFunction<T,R, E extends Exception> {

    R apply(T t) throws E;

}
