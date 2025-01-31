package com.openblocks.plugin.clickhouse.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;

import com.openblocks.sdk.models.DatasourceStructure.Column;
import com.openblocks.sdk.models.DatasourceStructure.Table;
import com.openblocks.sdk.models.DatasourceStructure.TableType;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class ClickHouseStructureParser {

    public static final String COLUMNS_QUERY = """
            SELECT
              table_name as table_name,
              ordinal_position as column_id,
              column_name as column_name,
              data_type as column_type
            FROM
              INFORMATION_SCHEMA.COLUMNS
            where
              table_schema = database()
            ORDER BY
              table_name,
              ordinal_position
                    """;

    public static void parseTableAndColumns(Map<String, Table> tablesByName, Statement statement) throws SQLException {
        try (ResultSet columnsResultSet = statement.executeQuery(COLUMNS_QUERY)) {
            while (columnsResultSet.next()) {
                String tableName = columnsResultSet.getString("table_name");

                Table table = tablesByName.computeIfAbsent(tableName, __ -> new Table(
                        TableType.TABLE, "", tableName,
                        new ArrayList<>(),
                        new ArrayList<>(),
                        new ArrayList<>()
                ));

                table.addColumn(new Column(
                        columnsResultSet.getString("column_name"),
                        columnsResultSet.getString("column_type"),
                        null,
                        false
                ));
            }
        }
    }
}
