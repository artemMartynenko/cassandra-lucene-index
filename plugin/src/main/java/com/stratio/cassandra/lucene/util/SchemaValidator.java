package com.stratio.cassandra.lucene.util;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.column.Column;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**Object for validating a {@link Schema} against a {@link CFMetaData}
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 *
 **/
public class SchemaValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaValidator.class);







    /** Validates the specified {@link Schema} against the specified {@link CFMetaData}.
     *
     * @param schema   a schema
     * @param metaData a table metadata
     */
    public static void validate(Schema schema, CFMetaData metaData){
        for(Mapper mapper:schema.mappers.values()){
            for (String column: mapper.mappedColumns){
                validate(metaData, column, mapper.field, mapper.supportedTypes, mapper.excludedTypes, mapper.supportsCollections);
            }
        }
    }






    public static void validate(CFMetaData metaData,
                                String column,
                                String field,
                                List<Class<?>> supportedTypes,
                                List<Class<?>> excludedTypes,
                                boolean supportsCollections){

        String cellName= Column.parseCellName(column);
        ColumnDefinition cellDefinition = metaData.getColumnDefinition(UTF8Type.instance.decompose(cellName));

        if(cellDefinition == null){
            throw new IndexException("No column definition '{}' for mapper '{}'", cellName, field);
        }

        if(cellDefinition.isStatic()){
            throw new IndexException("Lucene indexes are not allowed on static columns as '{}'", column);
        }

        AbstractType<?> cellType = cellDefinition.type;
        cellType.isCollection();
        List<String> udtNames = Column.parseUdtNames(column);
        if(udtNames == null || udtNames.isEmpty()){
            checkSupported(cellType, supportedTypes, excludedTypes, supportsCollections, cellName, field);
        }else {
            Column col = Column.of(cellName);
            AbstractType<?> currentType = cellType;
            for (int i = 0; i < udtNames.size(); i++) {
                col = col.withUDTName(udtNames.get(i));
                Optional<AbstractType<?>> type = childType(currentType, udtNames.get(i));
                if(!type.isPresent()){
                    throw new IndexException("No column definition '{}' for field '{}'",col.mapper, field);
                }else if(i == udtNames.size()-1){
                    checkSupported(type.get(), supportedTypes, excludedTypes, supportsCollections, col.mapper, field);
                }else {
                    currentType = type.get();
                }
            }
        }

    }










    private static void checkSupported(AbstractType<?> type,
                                       Collection<Class<?>> supportedTypes,
                                       Collection<Class<?>> excludedTypes,
                                       boolean supportCollections,
                                       String mapper,
                                       String field){
        if(!supports(type, supportedTypes, excludedTypes, supportCollections)){
            throw new IndexException("Type {} in column {} is not supported by mapper {}", type, mapper, field);
        }
    }







    public static Optional<AbstractType<?>> childType(AbstractType<?> parent, String child){
        if(parent instanceof ReversedType){
            return childType(((ReversedType<?>) parent).baseType, child);
        }else if(parent instanceof SetType){
            return childType(((SetType) parent).nameComparator(), child);
        }else if(parent instanceof ListType){
            return childType(((ListType) parent).valueComparator(), child);
        }else if (parent instanceof MapType && child.equals(Column.MAP_KEY_SUFFIX)){
            return Optional.of(((MapType) parent).getKeysType());
        }else if(parent instanceof MapType && child.equals(Column.MAP_VALUE_SUFFIX)){
            return Optional.of(((MapType) parent).valueComparator());
        }else if(parent instanceof MapType){
            return childType(((MapType) parent).valueComparator(), child);
        }else if(parent instanceof UserType){
            UserType userType = (UserType) parent;
            for (int i = 0; i < userType.fieldNames().size(); i++) {
                if(userType.fieldNameAsString(i).equals(child)){
                    return Optional.of(userType.fieldType(i));
                }
            }
            return Optional.empty();
        }else if(parent instanceof TupleType){
            TupleType tuple = (TupleType) parent;
            for (int i = 0; i < tuple.size(); i++) {
                if(String.valueOf(i).equals(child)){
                    return Optional.of(tuple.type(i));
                }
            }
            return Optional.empty();
        }else {
           return Optional.empty();
        }
    }



    public static boolean supports(AbstractType<?> candidate,
                            Collection<Class<?>> supportedTypes,
                            Collection<Class<?>> excludedTypes,
                            boolean supportsCollections){

        if(candidate instanceof ReversedType){
            return supports(((ReversedType<?>) candidate).baseType, supportedTypes, excludedTypes, supportsCollections);
        }else if(candidate instanceof SetType){
            return supportsCollections ? supports(((SetType) candidate).getElementsType(), supportedTypes, excludedTypes, supportsCollections) : false;
        }else if(candidate instanceof ListType){
            return supportsCollections ? supports(((ListType) candidate).getElementsType(), supportedTypes, excludedTypes, supportsCollections) : false;
        }else if(candidate instanceof MapType){
            return supportsCollections ? supports(((MapType) candidate).getValuesType(), supportedTypes, excludedTypes, supportsCollections) : false;
        }else {
            Class<?> nativeType = nativeType(candidate);
            if(excludedTypes.contains(nativeType)) {
                return false;
            }else {
                return supportedTypes.stream().anyMatch(aClass -> aClass.isAssignableFrom(nativeType));
            }

        }

    }





    public static Class<?> nativeType(AbstractType<?> validator){
        if(validator instanceof UTF8Type || validator instanceof AsciiType){
            return String.class;
        }else if(validator instanceof SimpleDateType || validator instanceof TimestampType){
            return Date.class;
        } else if(validator instanceof UUIDType || validator instanceof LexicalUUIDType || validator instanceof TimeUUIDType){
            return UUID.class;
        }else if(validator instanceof ShortType){
            return Short.class;
        }else if(validator instanceof ByteType){
            return Byte.class;
        }else if(validator instanceof  Int32Type){
            return Integer.class;
        }else if(validator instanceof LongType){
            return Long.class;
        }else if(validator instanceof IntegerType){
            return BigInteger.class;
        }else if(validator instanceof FloatType){
            return Float.class;
        }else if(validator instanceof DoubleType){
            return Double.class;
        }else if(validator instanceof DecimalType){
            return BigDecimal.class;
        }else if(validator instanceof BooleanType){
            return Boolean.class;
        }else if(validator instanceof BytesType){
            return ByteBuffer.class;
        }else if(validator instanceof InetAddressType){
            return InetAddress.class;
        }else {
            throw new IndexException("Unsupported Cassandra data type: "+validator.getClass());
        }

    }
}
