package com.openblocks.plugin.clickhouse;

import static com.openblocks.sdk.exception.PluginCommonError.DATASOURCE_GET_STRUCTURE_ERROR;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.pf4j.Extension;

import com.openblocks.plugin.clickhouse.utils.ClickHouseStructureParser;
import com.openblocks.plugin.sql.GeneralSqlExecutor;
import com.openblocks.plugin.sql.SqlBasedQueryExecutor;
import com.openblocks.sdk.exception.PluginException;
import com.openblocks.sdk.models.DatasourceStructure;
import com.openblocks.sdk.models.DatasourceStructure.Table;
import com.openblocks.sdk.plugin.common.sql.SqlBasedDatasourceConnectionConfig;
import com.openblocks.sdk.plugin.sqlcommand.GuiSqlCommand;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Extension
public class ClickHouseQueryExecutor extends SqlBasedQueryExecutor {

    public ClickHouseQueryExecutor() {
        super(new GeneralSqlExecutor(false));
    }

    @Override
    protected GuiSqlCommand parseSqlCommand(String guiStatementType, Map<String, Object> detail) {
        throw new UnsupportedOperationException("ClickHouse doesn't support GUI SQL Command yet");
    }

    @Override
    protected DatasourceStructure getDatabaseMetadata(Connection connection, SqlBasedDatasourceConnectionConfig connectionConfig) {

        Map<String, Table> tablesByName = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement()) {
            ClickHouseStructureParser.parseTableAndColumns(tablesByName, statement);
        } catch (SQLException throwable) {
            throw new PluginException(DATASOURCE_GET_STRUCTURE_ERROR, "DATASOURCE_GET_STRUCTURE_ERROR",
                    throwable.getMessage());
        }

        DatasourceStructure structure = new DatasourceStructure(new ArrayList<>(tablesByName.values()));
        for (Table table : structure.getTables()) {
            table.getKeys().sort(Comparator.naturalOrder());
        }
        return structure;


    }
}