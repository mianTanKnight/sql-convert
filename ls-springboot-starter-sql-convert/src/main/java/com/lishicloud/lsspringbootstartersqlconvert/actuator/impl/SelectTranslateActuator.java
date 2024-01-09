package com.lishicloud.lsspringbootstartersqlconvert.actuator.impl;

import com.lishicloud.lsspringbootstartersqlconvert.actuator.StandardSqlTranslateActuator;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;

import java.util.ArrayList;
import java.util.List;

/**
 * 标准SQL select语句执行器的实现。
 * 该执行器负责处理和转换 SELECT 类型的 SQL 语句。
 * 它通过解析和修改 SQL 语法树中的各个节点来实现特定的转换逻辑。
 * <p>
 * 此类提供两种访问模式: 对象和静态
 *
 * @author ztq
 */
public class SelectTranslateActuator extends StandardSqlTranslateActuator {

    public SelectTranslateActuator(SqlNode rootNode, SqlDialect dialect) {
        super(rootNode, dialect);
    }


    @Override
    public String translateSql(boolean fromOrJoin) throws SqlParseException {
        return extractAndModify(rootNode, dialect, fromOrJoin);
    }

    /**
     * 获取此执行器处理的 DML 类型。
     *
     * @return DML 类型，这里是 SELECT。
     */
    @Override
    protected SqlKind DML() {
        return SqlKind.SELECT;
    }


    /**
     * 对 SQL 语句进行提取和修改。
     *
     * @param sqlNode    待转换的 SQL 节点。
     * @param dialect    目标 SQL 方言。
     * @param fromOrJoin 指示 SQL 节点是否来源于 FROM 或 JOIN 子句。
     * @return 转换后的 SQL 字符串。
     */
    public static String extractAndModify(SqlNode sqlNode, SqlDialect dialect, boolean fromOrJoin) throws SqlParseException {
        SqlNode result = extractAndModifySourceTableInSelectSql(sqlNode, fromOrJoin);
        assert result != null;
        return result.toSqlString(dialect).getSql();
    }

    /**
     * 递归地提取和修改 SELECT SQL 语句中的源表。
     *
     * @param sqlNode    待处理的 SQL 节点。
     * @param fromOrJoin 指示当前节点是否来自 FROM 或 JOIN 子句。
     * @return 修改后的 SQL 节点。
     */
    public static SqlNode extractAndModifySourceTableInSelectSql(SqlNode sqlNode, boolean fromOrJoin) throws SqlParseException {
        if (sqlNode == null) {
            return null;
        }
        final SqlKind sqlKind = sqlNode.getKind();
        if (SqlKind.SELECT.equals(sqlKind)) {
            SqlSelect selectNode = (SqlSelect) sqlNode;
            SqlNode modifiedFrom = extractAndModifySourceTableInSelectSql(selectNode.getFrom(), true);
            selectNode.setFrom(modifiedFrom);
            if (selectNode.getSelectList() != null) {
                List<SqlNode> modifiedSelectList = new ArrayList<>();
                for (SqlNode item : selectNode.getSelectList()) {
                    if (item instanceof SqlCall) {
                        modifiedSelectList.add(extractAndModifySourceTableInSelectSql(item, false));
                    } else {
                        //处理其他列名 他们的类型是IDENTIFIER 但他们来至FROM
                        modifiedSelectList.add(processColumnName(item));
                    }
                }
                selectNode.setSelectList(new SqlNodeList(modifiedSelectList, selectNode.getSelectList().getParserPosition()));
            }
            // 处理 WHERE 子句
            if (selectNode.getWhere() != null) {
                SqlNode modifiedWhere = extractAndModifySourceTableInSelectSql(selectNode.getWhere(), false);
                selectNode.setWhere(modifiedWhere);
            }
            // 处理 HAVING 子句
            if (selectNode.getHaving() != null) {
                SqlNode modifiedHaving = extractAndModifySourceTableInSelectSql(selectNode.getHaving(), false);
                selectNode.setHaving(modifiedHaving);
            }
            return selectNode;
        }
        if (SqlKind.JOIN.equals(sqlKind)) {
            SqlJoin sqlJoin = (SqlJoin) sqlNode;
            SqlNode modifiedLeft = extractAndModifySourceTableInSelectSql(sqlJoin.getLeft(), true);
            SqlNode modifiedRight = extractAndModifySourceTableInSelectSql(sqlJoin.getRight(), true);
            sqlJoin.setLeft(modifiedLeft);
            sqlJoin.setRight(modifiedRight);
            return sqlJoin;
        }
        if (SqlKind.AS.equals(sqlKind)) {
            SqlCall sqlCall = (SqlCall) sqlNode;
            SqlNode modifiedOperand = extractAndModifySourceTableInSelectSql(sqlCall.operand(0), fromOrJoin);
            sqlCall.setOperand(0, modifiedOperand);
            return sqlCall;
        }
        // 处理表名
        if (SqlKind.IDENTIFIER.equals(sqlKind) && fromOrJoin) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
            return processTableName(sqlIdentifier);
        }
        if (sqlNode instanceof SqlCall) {
            SqlCall call = (SqlCall) sqlNode;
            // 使用 FunctionTranslate 对函数调用进行转换
            SqlNode modifiedCall = processFunction(call);
            if (!modifiedCall.equals(call)) {
                // 如果发生了转换，返回修改后的函数调用
                return modifiedCall;
            }
            // 如果没有转换规则，对操作数进行递归处理
            List<SqlNode> modifiedOperands = new ArrayList<>();
            for (SqlNode operand : call.getOperandList()) {
                modifiedOperands.add(extractAndModifySourceTableInSelectSql(operand, false));
            }
            // 创建一个新的 SqlCall 节点
            return call.getOperator().createCall(new SqlNodeList(modifiedOperands, call.getParserPosition()));
        }
        return sqlNode;
    }

}