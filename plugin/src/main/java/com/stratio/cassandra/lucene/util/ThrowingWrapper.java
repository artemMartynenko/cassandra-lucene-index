package com.stratio.cassandra.lucene.util;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class ThrowingWrapper {


    public static <T,R> Function<T,R> wrapf(ThrowingFunction<T,R, Exception> function){
        return t -> {
            try {
                return function.apply(t);
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        };
    }

    public static <T> Consumer<T> wrapc(ThrowingConsumer<T, Exception> consumer){
        return t -> {
            try {
                consumer.accept(t);
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        };
    }

}
