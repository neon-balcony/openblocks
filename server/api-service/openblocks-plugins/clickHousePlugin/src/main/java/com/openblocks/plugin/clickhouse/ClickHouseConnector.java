package com.openblocks.plugin.clickhouse;

import org.apache.commons.lang3.StringUtils;
import org.pf4j.Extension;

import com.openblocks.plugin.clickhouse.model.ClickHouseDatasourceConfig;
import com.openblocks.plugin.sql.SqlBasedConnector;
import com.zaxxer.hikari.HikariConfig;

@Extension
public class ClickHouseConnector extends SqlBasedConnector<ClickHouseDatasourceConfig> {

    private static final String JDBC_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";

    public ClickHouseConnector() {
        super(50);
    }

    @Override
    protected String getJdbcDriver() {
        return JDBC_DRIVER;
    }

    @Override
    protected void setUpConfigs(ClickHouseDatasourceConfig datasourceConfig, HikariConfig config) {
        // Set authentication properties
        String username = datasourceConfig.getUsername();
        if (StringUtils.isNotEmpty(username)) {
            config.setUsername(username);
        }
        String password = datasourceConfig.getPassword();
        if (StringUtils.isNotEmpty(password)) {
            config.setPassword(password);
        }

        String host = datasourceConfig.getHost();
        long port = datasourceConfig.getPort();
        String database = datasourceConfig.getDatabase();
        String scheme = datasourceConfig.isUsingSsl() ? "https://" : "http://";
        String url = "jdbc:clickhouse:" + scheme + host + ":" + port + "/" + database;
        config.setJdbcUrl(url);

        config.addDataSourceProperty("characterEncoding", "UTF-8");
        config.addDataSourceProperty("useUnicode", "true");

        config.setReadOnly(datasourceConfig.isReadonly());
    }
}
