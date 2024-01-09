package com.lishicloud.lsspringbootstartersqlconvert.actuator;


import com.lishicloud.lsspringbootstartersqlconvert.translate.ColumnsTranslate;
import com.lishicloud.lsspringbootstartersqlconvert.translate.FunctionTranslate;
import com.lishicloud.lsspringbootstartersqlconvert.translate.TableNameTranslate;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

/**
 * 标准 SQL 解析执行器的抽象基类。
 * 定义了所有 SQL 翻译执行器必须实现的基本行为和结构。
 * 此类旨在被继承并根据特定的数据操作语言（DML）类型进行定制。
 *
 * @author ztq
 */
@Slf4j
public abstract class StandardSqlTranslateActuator implements TranslateActuator {

    protected final SqlNode rootNode;

    protected final SqlDialect dialect;

    protected StandardSqlTranslateActuator(SqlNode rootNode, SqlDialect dialect) {
        this.rootNode = rootNode;
        this.dialect = dialect;
    }

    /**
     * 获取由此执行器处理的特定 SQL 数据操作语言（DML）类型。
     *
     * @return 处理的 SqlKind 类型。例如，对于 SELECT 语句的处理器，应返回 SqlKind.SELECT。
     */
    protected abstract SqlKind DML();


    protected static SqlNode processFunction(SqlNode functionNode) {
        return FunctionTranslate.doTranslateOfPipeline(functionNode);
    }

    protected static SqlNode processColumnName(SqlNode item) {
        return ColumnsTranslate.doTranslateOfPipeline(item);
    }

    protected static SqlNode processTableName(SqlIdentifier sqlIdentifier) {
        return TableNameTranslate.doTranslateOfPipeline(sqlIdentifier);
    }

    @Override
    public String translateActuatorSql() throws SqlParseException {
        return translateSql(true);
    }

    public abstract String translateSql(boolean fromOrJoin) throws SqlParseException;


    @Override
    public int sqlMode() {
        return STANDARD;
    }

}
