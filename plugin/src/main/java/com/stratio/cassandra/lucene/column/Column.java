package com.stratio.cassandra.lucene.column;

import com.google.common.collect.Lists;
import com.stratio.cassandra.lucene.IndexException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SimpleDateType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * A cell of a CQL3 logic column, which in most cases is different from a storage engine column.
 *
 * @param cell  the name of the base cell
 * @param udt   the UDT suffix
 * @param map   the map suffix
 * @param value the optional value
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


    private String cell;
    private String udt;
    private String map;
    private Optional<?> value;
    private String mapper;
    private String field;


    public Column(String cell) {
        this.cell = Optional.ofNullable(cell).orElseThrow(() -> new IndexException("Cell name shouldn't be blank"));
    }

    public Column(String cell, Object value) {
        this.cell = Optional.ofNullable(cell).orElseThrow(() -> new IndexException("Cell name shouldn't be blank"));
        this.value = Optional.ofNullable(value);
    }

    public Column(String cell, String udt, String map, Object value) {
        this.cell = Optional.ofNullable(cell).orElseThrow(() -> new IndexException("Cell name shouldn't be blank"));
        this.udt = Optional.ofNullable(udt).orElse("");
        this.map = Optional.ofNullable(map).orElse("");
        this.value = Optional.ofNullable(value);
        this.mapper = cell.concat(udt);
        this.field = mapper.concat(map);
    }

    public static Column apply(String cell) {
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
            return null;
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

    /** Returns `true` if the value is not defined, `false` otherwise. */
    public boolean isEmpty() {
        return !value.isPresent();
    }

    /** Returns the value, or null if it is not defined. */
    public Object getValueOrNull() {
        return value.orElse(null);
    }

    /** Returns a copy of this with the specified name appended to the list of UDT names. */
    public Column withUDTName(String name) {
        return new Column(this.cell, this.udt + UDT_SEPARATOR + name, this.map, value.get());
    }

    /** Returns a copy of this with the specified name appended to the list of map names. */
    public Column withMapName(String name) {
        return new Column(this.cell, this.udt, this.map + MAP_SEPARATOR + name, value.get());
    }

    /** Returns a copy of this with the specified value. */
    public Column withValue(Object value) {
        return new Column(this.cell, this.udt, this.map, value);
    }

    /** Returns a copy of this with the specified decomposed value. */
    public Column withValue(ByteBuffer bb, AbstractType<?> t) {
        return withValue(compose(bb, t));
    }

    /** Returns the name for fields. */
    public String fieldName(String field) {
        return field.concat(map);
    }

    /** Returns a [[Columns]] composed by this and the specified column. */
    public Columns combine(Column column){
        return new Columns(Lists.newArrayList(this, column));
    }

    /** Returns a [[Columns]] composed by this and the specified columns. */
    public Columns combine(Columns columns){
        List<Column> composed = Lists.newArrayList(columns.getColumns());
        composed.add(this);
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
    public String toString() {
        return "Column{" +
                "cell='" + cell + '\'' +
                ", udt='" + udt + '\'' +
                ", map='" + map + '\'' +
                ", value=" + value +
                ", mapper='" + mapper + '\'' +
                ", field='" + field + '\'' +
                '}';
    }
}
