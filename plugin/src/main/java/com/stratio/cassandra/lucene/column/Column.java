package com.stratio.cassandra.lucene.column;

import com.google.common.collect.Lists;
import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;

/**
 * A cell of a CQL3 logic column, which in most cases is different from a storage engine column.
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko
 **/
public class Column {

    /**
     * The name suffix for CQL map keys.
     */
    public static final String MAP_KEY_SUFFIX = "_key";

    /**
     * The name suffix for CQL map values.
     */
    public static final String MAP_VALUE_SUFFIX = "_value";

    private static final String UDT_SEPARATOR = ".";
    private static final String MAP_SEPARATOR = "$";
    private static final String UDT_PATTERN = Pattern.quote(UDT_SEPARATOR);


    public static Column of(String cell) {
        return new Column(cell);
    }

    public static String parseCellName(String name) {
        int udtSuffixStart = name.indexOf(UDT_SEPARATOR);
        if (udtSuffixStart < 0) {
            int mapSuffixStart = name.indexOf(MAP_SEPARATOR);
            return mapSuffixStart < 0 ? name : name.substring(0, mapSuffixStart);
        } else {
            return name.substring(0, udtSuffixStart);
        }
    }

    public static String parseMapperName(String name){
        int mapSuffixStart = name.indexOf(MAP_SEPARATOR);
        return mapSuffixStart < 0 ? name : name.substring(0 ,mapSuffixStart);
    }

    public static List<String> parseUdtNames(String name){
        int udtSuffixStart = name.indexOf(UDT_SEPARATOR);
        if(udtSuffixStart >= 0 ) {
            int mapSuffixStart = name.indexOf(MAP_SEPARATOR);
            String udtSuffix = mapSuffixStart < 0 ? name.substring( udtSuffixStart + 1) : name.substring( udtSuffixStart +1 , mapSuffixStart);
            return Arrays.asList(udtSuffix.split(UDT_PATTERN));
        } else {
            return Collections.emptyList();
        }
    }

    public static Object compose(ByteBuffer bb, AbstractType<?> t) {
        if (t instanceof SimpleDateType) {
            SimpleDateType simpleDateType = (SimpleDateType) t;
            return new Date(simpleDateType.toTimeInMillis(bb));
        } else {
            return t.compose(bb);
        }
    }



    public String cell;
    public String udt;
    public String map;
    public Optional<?> value;
    public String mapper;
    public String field;

    /**
     * @param cell  the name of the base cell
     * @param udt   the UDT suffix
     * @param map   the map suffix
     * @param value the optional value
     */
    public Column(String cell, String udt, String map, Optional<?> value) {
        this.cell = Optional.ofNullable(cell).orElseThrow(() -> new IndexException("Cell name shouldn't be blank"));
        this.udt = Optional.ofNullable(udt).orElse("");
        this.map = Optional.ofNullable(map).orElse("");
        this.value = value;
        this.mapper = udt != null ? cell.concat(udt) : cell;
        this.field = map != null ? mapper.concat(map) : mapper;
    }

    public Column(String cell) {
        this(cell, StringUtils.EMPTY, StringUtils.EMPTY, Optional.empty());
    }

    public Column(String cell, Optional<?> value) {
       this(cell, null, null, value);
    }

    /** Returns `true` if the value is not defined, `false` otherwise. */
    public boolean isEmpty() {
        return !value.isPresent();
    }

    /** Returns the value, or null if it is not defined. */
    public Object valueOrNull() {
        return value.orElse(null);
    }

    /** Returns a copy of this with the specified name appended to the list of UDT names. */
    public Column withUDTName(String name) {
        return new Column(this.cell, this.udt + UDT_SEPARATOR + name, this.map, value);
    }

    /** Returns a copy of this with the specified name appended to the list of map names. */
    public Column withMapName(String name) {
        return new Column(this.cell, this.udt, this.map + MAP_SEPARATOR + name, value);
    }

    /** Returns a copy of this with the specified value. */
    public Column withValue(Object value) {
        return new Column(this.cell, this.udt, this.map, Optional.ofNullable(value));
    }

    /** Returns a copy of this with the specified decomposed value. */
    public Column withValue(ByteBuffer bb, AbstractType<?> t) {
        return withValue(compose(bb, t));
    }

    /** Returns the name for fields. */
    public String fieldName(String field) {
        return field.concat(map);
    }

    /** Returns a {@link Columns} composed by this and the specified column. */
    public Columns at(Column column){
        return new Columns(Lists.newArrayList(this, column));
    }

    /** Returns a {@link Columns} composed by this and the specified columns. */
    public Columns at(Columns columns){
        List<Column> composed = new ArrayList<>(columns.size() +1 );
        composed.add(this);
        composed.addAll(columns.getColumns());
        return new Columns(composed);
    }


    public String getCell() {
        return cell;
    }

    public void setCell(String cell) {
        this.cell = cell;
    }

    public String getUdt() {
        return udt;
    }

    public void setUdt(String udt) {
        this.udt = udt;
    }

    public String getMap() {
        return map;
    }

    public void setMap(String map) {
        this.map = map;
    }

    public Object getValue() {
        return value.orElse(null);
    }

    public void setValue(Object value) {
        this.value = Optional.ofNullable(value);
    }

    public String getMapper() {
        return mapper;
    }

    public void setMapper(String mapper) {
        this.mapper = mapper;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return Objects.equals(cell, column.cell) &&
                Objects.equals(udt, column.udt) &&
                Objects.equals(map, column.map) &&
                Objects.equals(value, column.value) &&
                Objects.equals(mapper, column.mapper) &&
                Objects.equals(field, column.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cell, udt, map, value, mapper, field);
    }

    @Override
    public String toString() {
        return "Column{" +
                "cell='" + cell + '\'' +
                ", field='" + field + '\'' +
                ", value=" + value +
                '}';
    }
}
