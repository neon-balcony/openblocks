package com.openblocks.plugin.sql;

import java.util.List;

import com.openblocks.sdk.plugin.sqlcommand.command.UpdateOrDeleteSingleCommandRenderResult;

public class StatementInput {

    private final boolean preparedStatement;
    private final String sql;
    private final List<Object> params;

    private StatementInput(boolean preparedStatement, String sql, List<Object> params) {
        this.preparedStatement = preparedStatement;
        this.sql = sql;
        this.params = params;
    }

    public static StatementInput fromSql(boolean preparedStatement, String sql, List<Object> params) {
        return new StatementInput(preparedStatement, sql, params);
    }

    public static StatementInput fromUpdateOrDeleteSingleRowSql(UpdateOrDeleteSingleCommandRenderResult updateOrDeleteSingle) {
        return new UpdateOrDeleteSingleRowStatementInput(updateOrDeleteSingle.sql(), updateOrDeleteSingle.bindParams(),
                updateOrDeleteSingle.getSelectQuery(), updateOrDeleteSingle.getSelectBindParams());
    }

    public boolean isPreparedStatement() {
        return preparedStatement;
    }

    public String getSql() {
        return sql;
    }

    public List<Object> getParams() {
        return params;
    }

    public static class UpdateOrDeleteSingleRowStatementInput extends StatementInput {

        private final String selectSql;
        private final List<Object> selectParams;

        private UpdateOrDeleteSingleRowStatementInput(String sql, List<Object> params, String selectSql, List<Object> selectParams) {
            super(true, sql, params);
            this.selectSql = selectSql;
            this.selectParams = selectParams;
        }

        public StatementInput getSelectInput() {
            return StatementInput.fromSql(isPreparedStatement(), selectSql(), selectBindParams());
        }

        public String selectSql() {
            return selectSql;
        }

        public List<Object> selectBindParams() {
            return selectParams;
        }
    }

    public class BatchStatementInput extends StatementInput {
        private final List<StatementInput> statementInputs;

        public BatchStatementInput(List<StatementInput> statementInputs) {
            super(false, null, null);
            this.statementInputs = statementInputs;
        }

        public List<StatementInput> getStatementInputs() {
            return statementInputs;
        }
    }

}
