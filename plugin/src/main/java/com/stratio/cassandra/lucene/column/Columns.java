package com.stratio.cassandra.lucene.column;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * An immutable sorted list of CQL3 logic [[Column]]s.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class Columns {


    private final List<Column> columns;


    /**
     * @param columns the [[Column]]s composing this
     */
    public Columns(List<Column> columns) {
        this.columns = columns;
    }

    /**
     * @constructor create a new empty columns.
     */
    public Columns() {
        this.columns = Collections.emptyList();
    }


    public boolean isEmpty() {
        return columns.isEmpty();
    }

    public int size(){
        return columns.size();
    }

    public void forEach(Consumer<Column> procedure) {
        columns.forEach(procedure);
    }

    /**
     * Returns a copy of this with the specified column prepended in O(1) time.
     */
    public Columns copyWithHead(Column column) {
        List<Column> newList = Lists.newArrayList(column);
        newList.addAll(columns);
        return new Columns(newList);
    }


    /**
     * Returns a copy of this with the specified column appended in O(n) time.
     */
    public Columns copyWithTail(Column column) {
        List<Column> newList = Lists.newArrayList(columns);
        newList.add(column);
        return new Columns(newList);
    }

    /**
     * Returns a copy of this with the specified columns appended.
     */
    public Columns copyAndCombine(Columns columns) {
        List<Column> newList = Lists.newArrayList(this.columns);
        newList.addAll(columns.columns);
        return new Columns(newList);
    }


    /** Returns the value of the first column with the specified mapper name. */
    public Object valueForField(String field) {
        return this.columns.stream().filter(column -> column.getField().equals(field))
                .findFirst().map(Column::getValue).orElse(null);
    }

    /** Runs the specified function over each column with the specified field name. */
    public void foreachWithMapper(String field, Consumer<Column> procedure) {
        String mapper = Column.parseMapperName(field);
        columns.forEach(column -> {
            if (column.getMapper().equals(mapper)) procedure.accept(column);
        });
    }

    /** Returns a copy of this with the specified column appended. */
    public Columns add(String cell){
      List<Column> newList = Lists.newArrayList(this.columns);
      newList.add(new Column(cell));
      return new Columns(newList);
    }

    /** Returns a copy of this with the specified column appended. */
    public Columns add(String cell, Object value){
        List<Column> newList = Lists.newArrayList(this.columns);
        newList.add(new Column(cell, value));
        return new Columns(newList);
    }

    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this.getClass());
        columns.forEach(column -> helper.add(column.getField(), column.getValue()));
        return helper.toString();
    }



    public static Columns empty(){
        return new Columns();
    }

    public static Columns newColumns(Iterable<Column> columns){
        return new Columns(Lists.newArrayList(columns));
    }

    public static Columns newColumns(Column... columns){
        return new Columns(Lists.newArrayList(columns));
    }



}
