package com.lishicloud.lsspringbootstartersqlconvert.actuator.impl;

import com.lishicloud.lsspringbootstartersqlconvert.actuator.StandardSqlTranslateActuator;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

/**
 * 标准SQL insert语句执行器实现
 * update 处理表名 + 关键字(但要注意批量插入)
 * @author ztq
 */
public class InsertTranslateActuator extends StandardSqlTranslateActuator {

    public InsertTranslateActuator(SqlNode rootNode, SqlDialect dialect) {
        super(rootNode, dialect);
    }

    @Override
    protected SqlKind DML() {
        return SqlKind.INSERT;
    }


    @Override
    public String translateSql( boolean fromOrJoin) throws SqlParseException {
        if (!(rootNode instanceof SqlInsert)) {
            // 如果不是 INSERT 语句，则不进行处理
            return null;
        }
        SqlInsert insertNode = (SqlInsert) rootNode;
        SqlNode targetTable = insertNode.getTargetTable();
        SqlNode source = insertNode.getSource();
        SqlNodeList columnList = insertNode.getTargetColumnList();
        // 处理表名
        if (targetTable instanceof SqlIdentifier) {
            targetTable = processTableName((SqlIdentifier) targetTable);
        }
        // 处理列名关键字
        if (columnList != null) {
            List<SqlNode> modifiedColumns = new ArrayList<>();
            for (SqlNode column : columnList) {
                modifiedColumns.add(processColumnName(column));
            }
            columnList = new SqlNodeList(modifiedColumns, columnList.getParserPosition());
        }
        SqlNodeList keywords = new SqlNodeList(SqlParserPos.ZERO);
        // 重构 SqlInsert 节点
        SqlInsert modifiedInsert = new SqlInsert(insertNode.getParserPosition(),
                keywords,
                targetTable,
                source,
                columnList);
        // 适用于不同数据库方言的转换
        return modifiedInsert.toSqlString(dialect).getSql();
    }


}
