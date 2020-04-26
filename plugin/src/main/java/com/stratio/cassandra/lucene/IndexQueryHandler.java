package com.stratio.cassandra.lucene;

import com.stratio.cassandra.lucene.partitioning.Partitioner;
import com.stratio.cassandra.lucene.search.Search;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.LuceneStorageProxy;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * {@link QueryHandler} to be used with Lucene searches.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmail.com
 **/
public class IndexQueryHandler implements QueryHandler {

    private static final Method processResult;

    static {
        try {
            processResult = SelectStatement.class.getDeclaredMethod("processResults", PartitionIterator.class, QueryOptions.class, int.class, int.class);
            processResult.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public ResultMessage process(String query, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
        return null;
    }

    @Override
    public ResultMessage.Prepared prepare(String query, QueryState state, Map<String, ByteBuffer> customPayload) throws RequestValidationException {
        return null;
    }

    @Override
    public ParsedStatement.Prepared getPrepared(MD5Digest id) {
        return null;
    }

    @Override
    public ParsedStatement.Prepared getPreparedForThrift(Integer id) {
        return null;
    }

    @Override
    public ResultMessage processPrepared(CQLStatement statement, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
        return null;
    }

    @Override
    public ResultMessage processBatch(BatchStatement statement, QueryState state, BatchQueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
        return null;
    }



    //TODO: continue from bottom.
    public ResultMessage execute(CQLStatement statement,
                                 QueryState state,
                                 QueryOptions options,
                                 long queryStartNanoTime){
        ResultMessage result = statement.execute(state, options, queryStartNanoTime);
        return result == null ? new ResultMessage.Void() : result;
    }


    public ResultMessage executeLuceneQuery(SelectStatement select,
                                            QueryState state,
                                            QueryOptions options,
                                            Map<RowFilter.Expression, Index> expressions,
                                            long queryStartNanoTime){

        if (expressions.size() > 1){
            throw new InvalidRequestException(
                    "Lucene index only supports one search expression per query.");
        }

        if(select.getPerPartitionLimit(options) < Integer.MAX_VALUE){
            throw new InvalidRequestException("Lucene index doesn't support PER PARTITION LIMIT");
        }

        // Validate expression
        Map.Entry<RowFilter.Expression, Index> expressionIndexEntry = expressions.entrySet().iterator().next();
        Search search = expressionIndexEntry.getValue().validate(expressionIndexEntry.getKey());

        // Get partitioner
        Partitioner partitioner = expressionIndexEntry.getValue().service().partitioner;

        int limit = select.getLimit(options);
        int pageSize = select.getSelection().isAggregate() && options.getPageSize() <= 0 ? SelectStatement.DEFAULT_PAGE_SIZE : options.getPageSize();

        if(search.requiresPostProcessing() && pageSize > 0 && pageSize < limit ){
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
            if (data!=null){
                data.close();
            }
        }
    }

}
