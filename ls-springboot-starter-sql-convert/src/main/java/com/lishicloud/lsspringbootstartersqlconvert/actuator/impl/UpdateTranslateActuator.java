package com.lishicloud.lsspringbootstartersqlconvert.actuator.impl;

import com.lishicloud.lsspringbootstartersqlconvert.actuator.StandardSqlTranslateActuator;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;

import java.util.ArrayList;
import java.util.List;

/**
 * 标准SQL update 语句执行器实现
 * update 处理表名 + 关键字
 *
 * @author ztq
 */
public class UpdateTranslateActuator extends StandardSqlTranslateActuator {

    public UpdateTranslateActuator(SqlNode rootNode, SqlDialect dialect) {
        super(rootNode, dialect);
    }

    @Override
    protected SqlKind DML() {
        return SqlKind.UPDATE;
    }


    @Override
    public String translateSql(boolean fromOrJoin) throws SqlParseException {
        if (!(rootNode instanceof SqlUpdate)) {
            // 如果不是 UPDATE 语句，则不进行处理
            return null;
        }
        SqlUpdate updateNode = (SqlUpdate) rootNode;
        SqlNode targetTable = updateNode.getTargetTable();
        SqlNodeList targetColumnList = updateNode.getTargetColumnList();
        SqlNode condition = updateNode.getCondition();
        SqlNodeList sourceExpressionList = updateNode.getSourceExpressionList();
        SqlSelect sourceSelect = updateNode.getSourceSelect();
        SqlIdentifier alias = updateNode.getAlias();
        // 处理表名
        if (targetTable instanceof SqlIdentifier) {
            targetTable = processTableName((SqlIdentifier) targetTable);
        }
        // 处理列名关键字
        if (targetColumnList != null) {
            List<SqlNode> modifiedColumns = new ArrayList<>();
            for (SqlNode column : targetColumnList) {
                modifiedColumns.add(processColumnName(column));
            }
            targetColumnList = new SqlNodeList(modifiedColumns, targetColumnList.getParserPosition());
        }
        // 重构 SqlUpdate 节点
        SqlUpdate modifiedUpdate = new SqlUpdate(updateNode.getParserPosition(),
                targetTable,
                targetColumnList,
                sourceExpressionList,
                condition,
                sourceSelect,
                alias);

        // 适用于不同数据库方言的转换
        return modifiedUpdate.toSqlString(dialect).getSql();
    }
}
