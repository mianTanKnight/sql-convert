package com.lishicloud.lsspringbootstartersqlconvert.actuator;

import org.apache.calcite.sql.parser.SqlParseException;

/**
 * TranslateActuator 接口定义了 SQL 翻译执行器的核心功能。
 * 它负责将 SQL 语句转换为特定数据库方言的形式。
 * <p>
 * 当前实现状态：
 * - 标准 SQL：已实现（参见 {@link com.lishicloud.lsspringbootstartersqlconvert.actuator.StandardSqlTranslateActuator}）。
 * - 增强 SQL：未实现
 * - 非标准 SQL：未实现
 *
 * @author ztq
 */
public interface TranslateActuator {

    int STANDARD = 0;

    int ENHANCE = 1;

    int OFF_STANDARD = 2;


    /**
     * 将 SQL 语句转换为目标数据库方言。
     *
     * @return 转换后的 SQL 语句。
     * @throws SqlParseException 如果 SQL 语句解析过程中遇到问题。
     */
    String translateActuatorSql() throws SqlParseException;


    /**
     * SQL模式
     */
    int sqlMode();


}
