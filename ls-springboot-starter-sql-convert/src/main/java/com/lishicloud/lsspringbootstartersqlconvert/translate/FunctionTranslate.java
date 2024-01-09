package com.lishicloud.lsspringbootstartersqlconvert.translate;


import com.google.common.collect.Maps;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.*;

/**
 * 针对 Columns `s Function *  可能会存在于多个SQLNode中
 * 例如 select ,having
 *
 * @author ztq
 */
public abstract class FunctionTranslate extends Pipeline implements Translate {

    /**
     * 领域私有(实现 FunctionTranslate)
     */
    protected static HashMap<String, FunctionTranslate> rowsFunctionOfTranslateUnmodifiableMap;

    /**
     * 在构造函数中，将此类的实例与目标函数名称相关联并存储到映射中。
     */
    public FunctionTranslate() {
        super();
        synchronized (REGISTER_LOCK) {
            assert this.valid();
            if (rowsFunctionOfTranslateUnmodifiableMap == null) {
                rowsFunctionOfTranslateUnmodifiableMap = Maps.newHashMap();
            }
            rowsFunctionOfTranslateUnmodifiableMap.put(this.targetFunctionName().toUpperCase(Locale.ROOT), this);
        }
    }

    public boolean valid() {
        return StringUtils.isBlank(targetFunctionName());
    }

    /**
     * 返回此转换所针对的原始 SQL 函数名称。
     *
     * @return 目标函数的名称。
     */
    protected abstract String targetFunctionName();


    /**
     * 返回转换后的新 SQL 函数名称。
     *
     * @return 新函数的名称。
     */
    protected String newFunctionName(){
        return null;
    };


    /**
     * 提供源参数列表 例如 A(a,b,c) -> A(c,b,a) ,(b,a,c)....
     * 接受自定义顺序参数列表 严格按顺序组织新的函数
     */
    protected String[] simpleTranslateOfArgs(String[] sourceArgs) {
        return null;
    }


    /**
     * 用于复杂函数的构建 直接接受处理完之后的函数体
     * 例如MYSQL IF(expression,A,B) -> CASE WHEN a THEN b ELSE c END
     */
    protected String customTranslateOfArgs(String[] sourceArgs) {
        return null;
    }

    /**
     * 根据提供的函数名称执行 SQL 函数的转换。
     * 如果映射中存在相应的转换规则，则应用这些规则来转换 SQL 函数。
     *
     * @param sqlNode 待转换的 SQL 节点。
     * @return 转换后的 SQL 节点。如果未找到转换规则，则返回原始节点。
     */
    public static SqlNode doTranslateOfPipeline(SqlNode sqlNode) {
        if (!(sqlNode instanceof SqlCall)) {
            return sqlNode;
        }
        SqlCall call = (SqlCall) sqlNode;
        String functionName = call.getOperator().getName();
        // 检查是否有对应的转换规则
        if (!CollectionUtils.isEmpty(rowsFunctionOfTranslateUnmodifiableMap) && rowsFunctionOfTranslateUnmodifiableMap.containsKey(functionName.toUpperCase(Locale.ROOT))) {
            FunctionTranslate sqlFunctionTranslator = rowsFunctionOfTranslateUnmodifiableMap.get(functionName);
            // 获取新函数名和参数
            String newFunctionName = sqlFunctionTranslator.newFunctionName();
            if (StringUtils.isBlank(newFunctionName)) {
                newFunctionName = functionName;
            }
            List<SqlNode> operandList = call.getOperandList();
            String[] sourceArgs = new String[operandList.size()];
            for (int i = 0; i < operandList.size(); i++) {
                sourceArgs[i] = operandList.get(i).toString();
            }
            String[] args;
            // 尝试使用自定义参数转换
            String customArgs = sqlFunctionTranslator.customTranslateOfArgs(sourceArgs);
            if (customArgs != null) {
                return createSqlNodeFromCustomExpression(customArgs, call.getParserPosition());
            } else {
                // 否则使用简单参数转换
                args = sqlFunctionTranslator.simpleTranslateOfArgs(sourceArgs);
            }
            if (null == args) {
                return sqlNode;
            }
            // 构建新的函数节点
            SqlParserPos pos = call.getParserPosition();
            SqlOperator operator = new SqlUnresolvedFunction(
                    new SqlIdentifier(newFunctionName, pos),
                    // 根据需要调整返回类
                    ReturnTypes.ARG0,
                    // 根据需要调整类型推断
                    InferTypes.FIRST_KNOWN,
                    // 根据需要调整操作数类型
                    OperandTypes.NILADIC,
                    null,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);

            return new SqlBasicCall(operator, Arrays.stream(args)
                    .map(arg -> new SqlIdentifier(arg, pos)).toArray(SqlNode[]::new), pos);
        }
        // 如果没有对应的规则，返回原始节点
        return sqlNode;
    }

    private static SqlNode createSqlNodeFromCustomExpression(String expression, SqlParserPos pos) {
        // 使用 Apache Calcite 的 SQL 解析器解析自定义表达式
        SqlParser parser = SqlParser.create(expression);
        try {
            return parser.parseExpression();
        } catch (SqlParseException e) {
            // 处理解析异常
            throw new RuntimeException("Failed to parse custom SQL expression: " + expression, e);
        }
    }

    /**
     * empty impl
     */
    @Override
    public SqlNode translate(SqlNode node) {
        return null;
    }

    @Override
    protected void destroy() {
        synchronized (DESTROY_LOCK) {
            if (rowsFunctionOfTranslateUnmodifiableMap != null) {
                // simple destroy, The rest of the work goes to GC
                rowsFunctionOfTranslateUnmodifiableMap = null;
            }
        }
    }

}
