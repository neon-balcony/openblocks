package com.openblocks.plugin.postgres;

import static com.openblocks.plugin.postgres.utils.PostgresResultParser.parseDatabaseStructure;
import static com.openblocks.sdk.exception.PluginCommonError.QUERY_ARGUMENT_ERROR;
import static com.openblocks.sdk.exception.PluginCommonError.QUERY_EXECUTION_ERROR;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.pf4j.Extension;

import com.openblocks.plugin.sql.SqlBasedQueryExecutor;
import com.openblocks.sdk.exception.PluginException;
import com.openblocks.sdk.models.DatasourceStructure;
import com.openblocks.sdk.plugin.common.sql.SqlBasedDatasourceConnectionConfig;
import com.openblocks.sdk.plugin.sqlcommand.GuiSqlCommand;
import com.openblocks.sdk.plugin.sqlcommand.command.postgres.PostgresBulkInsertCommand;
import com.openblocks.sdk.plugin.sqlcommand.command.postgres.PostgresBulkUpdateCommand;
import com.openblocks.sdk.plugin.sqlcommand.command.postgres.PostgresDeleteCommand;
import com.openblocks.sdk.plugin.sqlcommand.command.postgres.PostgresInsertCommand;
import com.openblocks.sdk.plugin.sqlcommand.command.postgres.PostgresUpdateCommand;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Extension
public class PostgresExecutor extends SqlBasedQueryExecutor {

    public PostgresExecutor() {
        super(new PostgresSqlExecutor());
    }

    protected GuiSqlCommand parseSqlCommand(String guiStatementType, Map<String, Object> detail) {
        return switch (guiStatementType.toUpperCase()) {
            case "INSERT" -> PostgresInsertCommand.from(detail);
            case "UPDATE" -> PostgresUpdateCommand.from(detail);
            case "DELETE" -> PostgresDeleteCommand.from(detail);
            case "BULK_INSERT" -> PostgresBulkInsertCommand.from(detail);
            case "BULK_UPDATE" -> PostgresBulkUpdateCommand.from(detail);
            default -> throw new PluginException(QUERY_ARGUMENT_ERROR, "INVALID_GUI_COMMAND_TYPE", guiStatementType);
        };
    }

    @Override
    protected DatasourceStructure getDatabaseMetadata(Connection connection, SqlBasedDatasourceConnectionConfig connectionConfig) {
        DatasourceStructure structure = new DatasourceStructure();
        Map<String, DatasourceStructure.Table> tablesByName = new LinkedHashMap<>();

        try (Statement statement = connection.createStatement()) {
            parseDatabaseStructure(tablesByName, statement);
        } catch (SQLException throwable) {
            throw new PluginException(QUERY_EXECUTION_ERROR, "QUERY_EXECUTION_ERROR", throwable.getMessage());
        }

        structure.setTables(new ArrayList<>(tablesByName.values()));
        for (DatasourceStructure.Table table : structure.getTables()) {
            table.getKeys().sort(Comparator.naturalOrder());
        }
        return structure;
    }

}
