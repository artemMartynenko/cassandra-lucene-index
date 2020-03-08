package com.stratio.cassandra.lucene.index;

import java.util.Objects;

/**
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class Tuple<T1,T2> {
    public final T1 _1;
    public final T2 _2;

    public Tuple(T1 t1, T2 t2) {
        _1 = t1;
        _2 = t2;
    }


    public T1 get_1() {
        return _1;
    }

    public T2 get_2() {
        return _2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple<?, ?> tuple = (Tuple<?, ?>) o;
        return Objects.equals(_1, tuple._1) &&
                Objects.equals(_2, tuple._2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_1, _2);
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "_1=" + _1 +
                ", _2=" + _2 +
                '}';
    }
}
