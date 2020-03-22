package com.stratio.cassandra.lucene.mapping;

import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.search.Search;
import com.stratio.cassandra.lucene.search.SearchBuilder;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.ScoreDoc;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;

/**
 * Class for several @{@link RowFilter.Expression} mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class ExpressionMapper {

    public CFMetaData tableMetadata;
    public IndexMetadata indexMetadata;
    public final String name;
    public final Optional<String> column;
    public final Set<ColumnDefinition> columns;
    public final Optional<ColumnDefinition> columnDefinition;

    public ExpressionMapper(CFMetaData tableMetadata, IndexMetadata indexMetadata) {
        this.tableMetadata = tableMetadata;
        this.indexMetadata = indexMetadata;
        name = indexMetadata.name;
        column = Optional.of(indexMetadata.options.get(IndexTarget.TARGET_OPTION_NAME)).filter(s -> !StringUtils.isBlank(s));
        columns = Sets.newConcurrentHashSet(tableMetadata.allColumns());
        columnDefinition = column.flatMap(name -> columns.stream().filter(cd -> cd.name.toString().equals(name)).findAny());
    }

    public static Search parse(String json) {
        return SearchBuilder.fromJson(json).build();
    }

    /**
     * Returns the first {@link Search} contained in the specified read command.
     *
     * @param command a command
     * @return the `string` JSON search represented by `command`
     * @throws IndexException if there is no such search
     */
    public Search search(ReadCommand command) {
        return parse(json(command));
    }


    /**
     * Returns the {@link Search} represented by the specified CQL expression.
     *
     * @param expression a expression
     * @return the `string` JSON search represented by `expression`
     * @throws IndexException if there is no such search
     */
    public Search search(RowFilter.Expression expression) {
        return parse(json(expression));
    }


    /**
     * Returns the first `string` JSON search contained in the specified read command.
     *
     * @param command a command
     * @return the `string` JSON search represented by `command`
     * @throws IndexException if there is no such expression
     */
    public String json(ReadCommand command) {
        return command.rowFilter().getExpressions().stream().filter(expression ->
                (expression instanceof RowFilter.CustomExpression && ((RowFilter.CustomExpression) expression).getTargetIndex().name.equals(name))
                        || supports(expression)
        ).map(expression -> {
            if (expression instanceof RowFilter.CustomExpression) {
                return ((RowFilter.CustomExpression) expression).getValue();
            } else {
                return expression.getIndexValue();
            }
        }).map(UTF8Type.instance::compose).findFirst().orElse(null);
    }


    /**
     * Returns the `string` JSON search represented by the specified CQL expression.
     *
     * @param expression a expression
     * @return the `string` JSON search represented by `expression`
     * @throws IndexException if there is no such expression
     */
    public String json(RowFilter.Expression expression) {

        ByteBuffer exprValue;
        if (expression instanceof RowFilter.CustomExpression && ((RowFilter.CustomExpression) expression).getTargetIndex().name.equals(name)) {
            exprValue = ((RowFilter.CustomExpression) expression).getValue();
        } else if (supports(expression)) {
            exprValue = expression.getIndexValue();
        } else {
            throw new IndexException("Unsupported expression " + expression.toString());
        }
        return UTF8Type.instance.compose(exprValue);
    }


    /**
     * Returns if the specified expression is targeted to this index
     *
     * @param expression a CQL query expression
     * @return `true` if `expression` is targeted to this index, `false` otherwise
     */
    public boolean supports(RowFilter.Expression expression) {
        return supports(expression.column(), expression.operator());
    }


    /**
     * Returns if a CQL expression with the specified column definition and operator is targeted to
     * this index.
     *
     * @param definition the expression column definition
     * @param operator   the expression operator
     * @return `true` if the expression is targeted to this index, `false` otherwise
     */
    public boolean supports(ColumnDefinition definition, Operator operator) {
        return operator.equals(Operator.EQ) && column.filter(s -> definition.name.toString().equals(s)).isPresent();
    }


    /**
     * Returns a copy of the specified [[RowFilter]] without any Lucene expressions.
     *
     * @param filter a row filter
     * @return a copy of `filter` without Lucene expressions
     */
    public RowFilter postIndexQueryFilter(RowFilter filter) {
        if (!column.isPresent()) return filter;
        RowFilter filteredRowFilter = filter;
        for (RowFilter.Expression expression : filter) {
            filteredRowFilter = supports(expression) ? filteredRowFilter.without(expression) : filteredRowFilter;
        }
        return filteredRowFilter;
    }


    /**
     * Returns a new row decorating the specified row with the specified Lucene score.
     *
     * @param row      the row to be decorated
     * @param score    a Lucene search score
     * @param nowInSec the operation time in seconds
     * @return a new decorated row
     */
    public Row decorate(Row row, ScoreDoc score, int nowInSec) {

        // Skip if there is no base column or score
        if (!columnDefinition.isPresent()) return row;

        // Copy row
        Row.Builder builder = BTreeRow.unsortedBuilder(nowInSec);
        builder.newRow(row.clustering());
        builder.addRowDeletion(row.deletion());
        builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo());
        row.cells().forEach(builder::addCell);

        // Add score cell
        long timestamp = row.primaryKeyLivenessInfo().timestamp();
        ByteBuffer scoreCellValue = UTF8Type.instance.decompose(String.valueOf(score.score));
        builder.addCell(BufferCell.live(columnDefinition.get(), timestamp, scoreCellValue));

        return builder.build();
    }


}
