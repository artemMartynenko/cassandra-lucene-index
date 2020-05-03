package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.partitioning.Partitioner;
import com.stratio.cassandra.lucene.search.Search;
import com.stratio.cassandra.lucene.util.TimeCounter;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.LuceneStorageProxy;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link QueryHandler} to be used with Lucene searches.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 **/
public class IndexQueryHandler implements QueryHandler {

    public static final Logger LOGGER = LoggerFactory.getLogger(IndexQueryHandler.class);

    private static final Method processResult;

    static {
        try {
            processResult = SelectStatement.class.getDeclaredMethod("processResults", PartitionIterator.class, QueryOptions.class, int.class, int.class);
            processResult.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    /** Sets this query handler as the Cassandra CQL query handler, replacing the previous one. */
    public static void activate(){
        synchronized (IndexQueryHandler.class) {
            if(!(ClientState.getCQLQueryHandler() instanceof IndexQueryHandler)){
                try {
                    Field field = ClientState.class.getDeclaredField("cqlQueryHandler");
                    field.setAccessible(true);
                    Field modifiers = Field.class.getDeclaredField("modifiers");
                    modifiers.setAccessible(true);
                    modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
                    field.set(null, new IndexQueryHandler());
                } catch (Exception e) {
                    throw new IndexException("Unable to set Lucene CQL query handler", e);
                }
            }
        }
    }

    @Override
    public ResultMessage.Prepared prepare(String query, QueryState state, Map<String, ByteBuffer> customPayload) throws RequestValidationException {
        return QueryProcessor.instance.prepare(query, state);
    }

    @Override
    public ParsedStatement.Prepared getPrepared(MD5Digest id) {
        return QueryProcessor.instance.getPrepared(id);
    }

    @Override
    public ParsedStatement.Prepared getPreparedForThrift(Integer id) {
        return QueryProcessor.instance.getPreparedForThrift(id);
    }

    @Override
    public ResultMessage processBatch(BatchStatement statement, QueryState state, BatchQueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
        return QueryProcessor.instance.processBatch(statement, state, options, customPayload, queryStartNanoTime);
    }

    @Override
    public ResultMessage processPrepared(CQLStatement statement, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
        QueryProcessor.metrics.preparedStatementsExecuted.inc();
        return processStatement(statement, state, options, queryStartNanoTime);
    }

    @Override
    public ResultMessage process(String query, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
        ParsedStatement.Prepared statement = QueryProcessor.getStatement(query, state.getClientState());
        options.prepare(statement.boundNames);
        CQLStatement prepared = statement.statement;
        if(prepared.getBoundTerms() != options.getValues().size()){
            throw new InvalidRequestException("Invalid amount of bind variables");
        }
        if(!state.getClientState().isInternal){
            QueryProcessor.metrics.regularStatementsExecuted.inc();
        }
        return processStatement(prepared, state, options, queryStartNanoTime);
    }








    public ResultMessage processStatement(CQLStatement statement,
                                          QueryState state,
                                          QueryOptions options,
                                          long queryStartNanoTime){

        LOGGER.trace("Process {} @CL.{}", statement, options.getConsistency());
        ClientState clientState = state.getClientState();
        statement.checkAccess(clientState);
        statement.validate(clientState);

        // Intercept Lucene index searches
        if(statement instanceof SelectStatement){
            SelectStatement select = (SelectStatement) statement;
            Map<RowFilter.Expression, Index> expressions = luceneExpressions(select, options);
            if(!expressions.isEmpty()){
                TimeCounter time = TimeCounter.start();
                try {
                    return executeLuceneQuery(select, state, options, expressions, queryStartNanoTime);
                } catch (Exception e){
                    throw new IndexException(e);
                }finally {
                    LOGGER.debug("Lucene search total time: {}\n", time.toString());
                }
            }

        }

        return execute(statement, state, options, queryStartNanoTime);

    }



    public Map<RowFilter.Expression, Index> luceneExpressions(SelectStatement select, QueryOptions options) {
        Map<RowFilter.Expression, Index> map = new LinkedHashMap<>();
        List<RowFilter.Expression> expressions = select.getRowFilter(options).getExpressions();
        ColumnFamilyStore columnFamilyStore = Keyspace.open(select.keyspace()).getColumnFamilyStore(select.columnFamily());
        List<Index> indexes = columnFamilyStore.indexManager.listIndexes().stream().map(index -> (Index) index).collect(Collectors.toList());

        expressions.forEach(expression -> {
            if (expression instanceof RowFilter.CustomExpression) {
                String clazz = ((RowFilter.CustomExpression) expression).getTargetIndex().options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
                if (clazz.equals(Index.class.getCanonicalName())) {
                    Index index = (Index) columnFamilyStore.indexManager.getIndex(((RowFilter.CustomExpression) expression).getTargetIndex());
                    map.put(expression, index);
                }
            } else {
                indexes.stream().filter(index -> index.supportsExpression(expression.column(), expression.operator())).forEach(index -> map.put(expression, index));
            }
        });
        return map;
    }


    public ResultMessage execute(CQLStatement statement,
                                 QueryState state,
                                 QueryOptions options,
                                 long queryStartNanoTime) {
        ResultMessage result = statement.execute(state, options, queryStartNanoTime);
        return result == null ? new ResultMessage.Void() : result;
    }


    public ResultMessage executeLuceneQuery(SelectStatement select,
                                            QueryState state,
                                            QueryOptions options,
                                            Map<RowFilter.Expression, Index> expressions,
                                            long queryStartNanoTime) {

        if (expressions.size() > 1) {
            throw new InvalidRequestException(
                    "Lucene index only supports one search expression per query.");
        }

        if (select.getPerPartitionLimit(options) < Integer.MAX_VALUE) {
            throw new InvalidRequestException("Lucene index doesn't support PER PARTITION LIMIT");
        }

        // Validate expression
        Map.Entry<RowFilter.Expression, Index> expressionIndexEntry = expressions.entrySet().iterator().next();
        Search search = expressionIndexEntry.getValue().validate(expressionIndexEntry.getKey());

        // Get partitioner
        Partitioner partitioner = expressionIndexEntry.getValue().service.partitioner;

        int limit = select.getLimit(options);
        int pageSize = select.getSelection().isAggregate() && options.getPageSize() <= 0 ? SelectStatement.DEFAULT_PAGE_SIZE : options.getPageSize();

        if (search.requiresPostProcessing() && pageSize > 0 && pageSize < limit) {
            return executeSortedLuceneQuery(select, state, options, partitioner, queryStartNanoTime);
        } else {
            return execute(select, state, options, queryStartNanoTime);
        }
    }


    public ResultMessage.Rows executeSortedLuceneQuery(SelectStatement select,
                                                       QueryState state,
                                                       QueryOptions options,
                                                       Partitioner partitioner,
                                                       long queryStartNanoTime) {

        // Check consistency level
        ConsistencyLevel consistency = options.getConsistency();
        RequestValidations.checkNotNull(consistency, "Invalid query. Empty consistency level");
        consistency.validateForRead(select.keyspace());


        int now = FBUtilities.nowInSeconds();
        int limit = select.getLimit(options);
        int userPerPartitionLimit = select.getPerPartitionLimit(options);
        int pageSize = options.getPageSize();

        IndexPagingState pagingState = IndexPagingState.build(options.getPagingState(), limit);
        int remaining = Math.min(pageSize, pagingState.remaining);
        ReadQuery query = select.getQuery(options, now, remaining, userPerPartitionLimit, pageSize);
        pagingState.rewrite(query);

        PartitionIterator data = null;

        try {
            if (query instanceof SinglePartitionReadCommand.Group) {
                data = LuceneStorageProxy.read((SinglePartitionReadCommand.Group) query, consistency, queryStartNanoTime);
            } else {
                data = query.execute(consistency, state.getClientState(), queryStartNanoTime);
            }
            PartitionIterator processedData = pagingState.update(query, data, consistency, partitioner);
            ResultMessage.Rows rows = (ResultMessage.Rows) processResult.invoke(select, processedData, options, now, pageSize);
            return rows;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (data != null) {
                data.close();
            }
        }
    }

}
