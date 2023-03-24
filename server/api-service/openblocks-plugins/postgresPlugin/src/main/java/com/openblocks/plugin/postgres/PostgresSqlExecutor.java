package com.openblocks.plugin.postgres;

import static com.google.common.collect.Lists.newArrayList;
import static com.openblocks.plugin.postgres.utils.PostgresDataTypeUtils.castValueWithTargetType;
import static com.openblocks.plugin.postgres.utils.PostgresDataTypeUtils.extractExplicitCasting;
import static com.openblocks.plugin.postgres.utils.PostgresDataTypeUtils.getPostgresType;
import static com.openblocks.sdk.exception.PluginCommonError.QUERY_EXECUTION_ERROR;
import static com.openblocks.sdk.util.MustacheHelper.doPrepareStatement;
import static com.openblocks.sdk.util.MustacheHelper.extractMustacheKeysInOrder;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.openblocks.plugin.postgres.model.DataType;
import com.openblocks.plugin.postgres.utils.PostgresResultParser;
import com.openblocks.plugin.sql.GeneralSqlExecutor;
import com.openblocks.plugin.sql.StatementInput;
import com.openblocks.sdk.exception.PluginException;

public class PostgresSqlExecutor extends GeneralSqlExecutor {

    @Override
    protected void bindParam(int bindIndex, Object value, PreparedStatement preparedStatement, String bindKeyName) throws SQLException {
        if (value instanceof JsonNode jsonNode) {
            preparedStatement.setString(bindIndex, jsonNode.toString());
            return;
        }

        if (value instanceof Collection<?> collection) {
            if (CollectionUtils.isEmpty(collection)) {
                preparedStatement.setNull(bindIndex, Types.NULL);
                return;
            }

            // Find the type of the entries in the list
            Optional<?> first = collection.stream()
                    .filter(Objects::nonNull)
                    .findFirst();
            if (first.isEmpty()) {
                preparedStatement.setNull(bindIndex, Types.NULL);
                return;
            }
            String typeName = getPostgresType(first.get());

            // Create the Sql Array and set it.
            Array inputArray = preparedStatement.getConnection().createArrayOf(typeName, collection.toArray());
            preparedStatement.setArray(bindIndex, inputArray);
            return;
        }
        super.bindParam(bindIndex, value, preparedStatement, bindKeyName);
    }

    @Override
    protected List<Map<String, Object>> parseDataRows(ResultSet resultSet) throws SQLException {
        try {
            return PostgresResultParser.parseRows(resultSet);
        } catch (JsonProcessingException e) {
            throw new PluginException(QUERY_EXECUTION_ERROR, "QUERY_EXECUTION_ERROR", e.getMessage());
        }
    }

    @Override
    protected StatementInput getPreparedStatementInput(String query, Map<String, Object> requestParams) {
        List<String> mustacheKeysInOrder = extractMustacheKeysInOrder(query);
        String preparedSql = doPrepareStatement(query, mustacheKeysInOrder, requestParams);

        if (mustacheKeysInOrder.isEmpty()) {
            return StatementInput.fromSql(true, preparedSql, Collections.emptyList());
        }

        List<DataType> explicitCastDataTypes = extractExplicitCasting(preparedSql);
        List<Object> finalValues = convertExplicitDataTypes(requestParams, mustacheKeysInOrder, explicitCastDataTypes);
        return StatementInput.fromSql(true, preparedSql, finalValues);
    }

    private List<Object> convertExplicitDataTypes(Map<String, Object> requestParams, List<String> mustacheKeysInOrder,
            List<DataType> explicitCastDataTypes) {
        List<Object> finalValues = newArrayList();
        for (int i = 0; i < mustacheKeysInOrder.size(); i++) {
            String key = mustacheKeysInOrder.get(i);
            boolean containsKey = requestParams.containsKey(key);
            if (!containsKey) {
                throw new PluginException(QUERY_EXECUTION_ERROR, "BOUND_VALUE_NOT_MATCH", key);
            }

            Object value = requestParams.get(key);
            DataType targetType = explicitCastDataTypes.get(i);
            if (targetType != null) {
                finalValues.add(castValueWithTargetType(value, explicitCastDataTypes.get(i)));
            } else {
                finalValues.add(value);
            }
        }
        return finalValues;
    }

}
