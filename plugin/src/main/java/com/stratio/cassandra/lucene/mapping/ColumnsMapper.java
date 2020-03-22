package com.stratio.cassandra.lucene.mapping;

import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.column.Columns;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Maps Cassandra rows to [[Columns]].
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class ColumnsMapper {


    /** Returns [[Columns]] contained in the specified cell.
     *
     * @param cell a cell
     */
    static Columns columns(Cell cell){
        if(cell == null) return Columns.empty();
        String name = cell.column().name.toString();
        AbstractType<?> comparator = cell.column().type;
        ByteBuffer value = cell.value();
        Column column = Column.of(name);

        if(comparator instanceof SetType && !comparator.isFrozenCollection() ){
            AbstractType<?> itemComparator = ((SetType<?>) comparator).nameComparator();
            ByteBuffer itemValue = cell.path().get(0);
            return columns(column, itemComparator, itemValue);
        }else if(comparator instanceof ListType && !comparator.isFrozenCollection()){
            AbstractType<?> itemComparator = ((ListType<?>) comparator).valueComparator();
            return columns(column, itemComparator, value);
        }else if(comparator instanceof MapType && !comparator.isFrozenCollection()){
            AbstractType<?> valueType = ((MapType<?,?>) comparator).valueComparator();
            ByteBuffer keyValue = cell.path().get(0);
            AbstractType<?> keyType = ((MapType<?,?>) comparator).nameComparator();
            String keyName = keyType.compose(keyValue).toString();
            Column keyColumn = column.withUDTName(Column.MAP_KEY_SUFFIX).withValue(keyValue, keyType);
            Columns valueColumn = columns(column.withUDTName(Column.MAP_KEY_SUFFIX), valueType, value);
            Columns entryColumn = columns(column.withMapName(keyName), valueType , value);
            return keyColumn.at(valueColumn).plus(entryColumn);
        }else if(comparator instanceof UserType){
            CellPath cellPath = cell.path();
            if (cellPath == null){
                return columns(column, comparator, value);
            }else {
                short position = ByteBufferUtil.toShort(cellPath.get(0));
                String fieldName = ((UserType) comparator).fieldNameAsString(position);
                AbstractType<?> type  = ((UserType) comparator).type(position);
                return columns(column.withUDTName(fieldName), type, value);
            }
        }else {
            return columns(column, comparator, value);
        }
    }

    static Columns columns(Column column, AbstractType<?> serializer, ByteBuffer value){
        if(serializer instanceof SetType){
            return columns(column, (SetType<?>) serializer, value);
        }else if(serializer instanceof ListType){
            return columns(column, (ListType<?>) serializer, value);
        }else if(serializer instanceof MapType){
            return columns(column, (MapType<?, ?>) serializer, value);
        }else if(serializer instanceof UserType){
            return columns(column, (UserType) serializer, value);
        }else if(serializer instanceof TupleType){
            return columns(column, (TupleType) serializer, value);
        }else {
            return Columns.of(column.withValue(value, serializer));
        }
    }


    static Columns columns(Column column, SetType<?> set, ByteBuffer value){
        AbstractType<?> nameType = set.nameComparator();
        ByteBuffer bb = ByteBufferUtil.clone(value);
        Columns columns = Columns.empty();
        for(int i = 0 ; i < frozenCollectionSize(bb); i++){
            ByteBuffer itemValue = frozenCollectionValue(bb);
            columns = columns.plus(columns(column, nameType, itemValue));
        }
        return columns;
    }

    static Columns columns(Column column, ListType<?> list, ByteBuffer value){
        AbstractType<?> valueType = list.valueComparator();
        ByteBuffer bb = ByteBufferUtil.clone(value);
        //TODO: rewrite columns class for making less copies
        Columns columns = Columns.empty();
        for (int i = 0; i < frozenCollectionSize(bb); i++) {
            ByteBuffer itemValue = frozenCollectionValue(bb);
            columns = columns.plus(columns(column, valueType, itemValue));
        }
        return columns;
    }


    static Columns columns(Column column, MapType<?,?> map, ByteBuffer value){
        AbstractType<?> itemKeysType = map.nameComparator();
        AbstractType<?> itemValuesType = map.valueComparator();
        ByteBuffer bb = ByteBufferUtil.clone(value);
        Columns columns = Columns.empty();
        for (int i = 0; i < frozenCollectionSize(bb); i++) {
            ByteBuffer itemKey = frozenCollectionValue(bb);
            ByteBuffer itemValue = frozenCollectionValue(bb);
            String itemName = itemKeysType.compose(itemKey).toString();
            Column keyColumn = column.withUDTName(Column.MAP_KEY_SUFFIX).withValue(itemKey, itemKeysType);
            Columns valueColumn = columns(column.withUDTName(Column.MAP_KEY_SUFFIX), itemValuesType, itemValue);
            Columns entryColumn = columns(column.withMapName(itemName), itemValuesType, itemValue);
            columns = keyColumn.at(valueColumn).plus(entryColumn).plus(columns);
        }
        return columns;
    }

    static Columns columns(Column column, UserType udt, ByteBuffer value){
        ByteBuffer[] itemValues = udt.split(value);
        Columns columns = Columns.empty();
        for (int i = 0; i < udt.fieldNames().size(); i++) {
            ByteBuffer itemValue = itemValues[i];
            if(itemValue != null){
                String itemName = udt.fieldNameAsString(i);
                AbstractType<?> itemType = udt.fieldType(i);
                Column itemColumn =column.withUDTName(itemName);
                columns = columns(itemColumn, itemType, itemValue).plus(columns);
            }
        }
        return columns;
    }






    static Columns columns(Column column, TupleType tupleType, ByteBuffer value){
      ByteBuffer[] itemValues = tupleType.split(value);
      Columns columns = Columns.empty();
      for (int i = 0; i < tupleType.size(); i++){
          ByteBuffer itemValue = itemValues[i];
          if(itemValue != null){
              String itemName = String.valueOf(i);
              AbstractType<?> itemType = tupleType.type(i);
              Column itemColumn = column.withUDTName(itemName);
              columns = columns(itemColumn, itemType, itemValue).plus(columns);
          }
      }
      return columns;
    }

    private static int frozenCollectionSize(ByteBuffer bb){
        return CollectionSerializer.readCollectionSize(bb, ProtocolVersion.CURRENT);
    }

    private static ByteBuffer frozenCollectionValue(ByteBuffer bb){
       return CollectionSerializer.readValue(bb, ProtocolVersion.CURRENT);
    }




    private Schema schema;
    private CFMetaData metaData;


    private List<ColumnDefinition> keyColumns;
    private List<ColumnDefinition> clusteringColumns;
    private Set<String> mappedCells;



    /**
     *
     * @param schema   a schema
     * @param metaData a table metadata
     * */
    public ColumnsMapper(Schema schema, CFMetaData metaData) {
        this.schema = schema;
        this.metaData = metaData;
        this.mappedCells = schema.mappedCells();
        this.keyColumns = metaData.partitionKeyColumns().stream()
                .filter(columnDefinition -> mappedCells.contains(columnDefinition.name.toString()))
                .collect(Collectors.toList());

        this.clusteringColumns = metaData.clusteringColumns().stream()
                .filter(columnDefinition -> mappedCells.contains(columnDefinition.name.toString()))
                .collect(Collectors.toList());
    }


    /** Returns the mapped, not deleted at the specified time in seconds and not null [[Columns]]
     * contained in the specified row.
     *
     * @param key the partition key
     * @param row the row
     * @param now now in seconds
     */
    public Columns columns(DecoratedKey key, Row row, int now){
       return columns(key).plus(columns(row.clustering())).plus(columns(row,now));
    }


    /** Returns the mapped {@link Columns} contained in the specified partition key. */
    Columns columns(DecoratedKey key){
        ByteBuffer[] components;
        if(metaData.getKeyValidator() instanceof CompositeType){
            components = ((CompositeType) metaData.getKeyValidator()).split(key.getKey());
        }else {
            components = new ByteBuffer[]{key.getKey()};
        }
        Columns columns = Columns.empty();
        for(ColumnDefinition definition: keyColumns){
            String name = definition.name.toString();
            ByteBuffer value = components[definition.position()];
            AbstractType<?> valueType = definition.cellValueType();
            columns = columns.plusToHead(new Column(name).withValue(value,valueType));
        }
        return columns;
    }


    /** Returns the mapped {@link Columns} contained in the specified clustering key. */
    Columns columns(Clustering clustering){
        Columns columns = Columns.empty();
        for(ColumnDefinition definition: clusteringColumns){
            columns = columns.plus(ColumnsMapper.columns(Column.of(definition.name.toString()), definition.type, clustering.get(definition.position())));
        }
        return columns;
    }


    /** Returns the mapped, not deleted at the specified time in seconds and not null {@link Columns}
     * contained in the regular columns of the specified row.
     *
     * @param row a row
     * @param now now in seconds
     */
    Columns columns(Row row, int now){
        Columns columns = Columns.empty();
        for (ColumnDefinition definition: row.columns()) {
            if(definition.isComplex()){
               columns = this.columns(row.getComplexColumnData(definition), now).plus(columns);
            }else {
                columns = this.columns(row.getCell(definition), now).plus(columns);
            }
        }
        return columns;
    }


    /** Returns the mapped, not deleted at the specified time in seconds and not null {@link Columns}
     * contained in the specified complex column data.
     *
     * @param complexColumnData a complex column data
     * @param now               now in seconds
     */
    Columns columns(ComplexColumnData complexColumnData, int now){
        Columns columns = Columns.empty();
        for (Iterator<Cell> it = complexColumnData.iterator(); it.hasNext(); ) {
            Cell cell = it.next();
            columns = columns.plus(columns(cell,now));
        }
        return columns;
    }

    /** Returns the mapped, not deleted at the specified time in seconds and not null {@link Columns}
     * contained in the specified cell.
     *
     * @param cell a cell
     * @param now  now in seconds
     */
    Columns columns(Cell cell, int now){
        if(cell.isTombstone() || cell.localDeletionTime() <= now || !mappedCells.contains(cell.column().name.toString())){
            return Columns.empty();
        }else {
            return  ColumnsMapper.columns(cell);
        }
    }

}
