package com.lishicloud.lsspringbootstartersqlconvert.actuator.impl;

import com.lishicloud.lsspringbootstartersqlconvert.actuator.StandardSqlTranslateActuator;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;

/**
 * 标准SQL delete语句执行器实现
 * delete 只处理表名相关 不考虑 WHERE X ="", X是关键字的情况
 * @author ztq
 */
public class DeleteTranslateActuator extends StandardSqlTranslateActuator {

    public DeleteTranslateActuator(SqlNode rootNode, SqlDialect dialect) {
        super(rootNode, dialect);
    }

    @Override
    protected SqlKind DML() {
        return SqlKind.DELETE;
    }


    @Override
    public String translateSql(boolean fromOrJoin) throws SqlParseException {
        if (!(rootNode instanceof SqlDelete)) {
            // 如果不是 DELETE 语句，则不进行处理
            return null;
        }
        SqlDelete deleteNode = (SqlDelete) rootNode;
        SqlNode targetTable = deleteNode.getTargetTable();
        // 处理表名
        if (targetTable instanceof SqlIdentifier) {
            SqlNode modifiedTable = processTableName((SqlIdentifier) targetTable);
            deleteNode = new SqlDelete(deleteNode.getParserPosition(), modifiedTable, deleteNode.getCondition(), deleteNode.getSourceSelect(), deleteNode.getAlias());
        }
        // 适用于不同数据库方言的转换
        return deleteNode.toSqlString(dialect).getSql();
    }


}
