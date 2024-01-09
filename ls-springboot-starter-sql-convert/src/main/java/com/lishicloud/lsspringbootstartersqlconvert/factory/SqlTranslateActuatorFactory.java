package com.lishicloud.lsspringbootstartersqlconvert.factory;

import com.lishicloud.lsspringbootstartersqlconvert.actuator.TranslateActuator;
import com.lishicloud.lsspringbootstartersqlconvert.actuator.impl.DeleteTranslateActuator;
import com.lishicloud.lsspringbootstartersqlconvert.actuator.impl.InsertTranslateActuator;
import com.lishicloud.lsspringbootstartersqlconvert.actuator.impl.SelectTranslateActuator;
import com.lishicloud.lsspringbootstartersqlconvert.actuator.impl.UpdateTranslateActuator;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple Factory
 *
 * @author ztq
 */
public class SqlTranslateActuatorFactory {

    private static final Map<SqlKind, TranslateActuator> ACTUATORS = new HashMap<>();


    public static synchronized void putTranslate(SqlKind sqlKind,TranslateActuator translateActuator){
        ACTUATORS.put(sqlKind,translateActuator);
    }
    /**
     * 默认标准SQL
     */
    public static TranslateActuator getTranslateActuatorOfAuto(String sql, SqlDialect dialect) throws SqlParseException {
        SqlParser.Config build = SqlParser.config().withUnquotedCasing(Casing.UNCHANGED);
        SqlParser parser = SqlParser.create(sql,build);
        SqlNode sqlNode = parser.parseQuery();
        TranslateActuator translateActuator;
        switch (sqlNode.getKind()) {
            case INSERT:
                if (null == (translateActuator = ACTUATORS.get(SqlKind.INSERT))) {
                    translateActuator = new InsertTranslateActuator(sqlNode, dialect);
                    putTranslate(SqlKind.INSERT,translateActuator);
                }
                return translateActuator;
            case UPDATE:
                if (null == (translateActuator = ACTUATORS.get(SqlKind.UPDATE))) {
                    translateActuator = new UpdateTranslateActuator(sqlNode, dialect);
                    putTranslate(SqlKind.UPDATE,translateActuator);
                }
                return translateActuator;
            case DELETE:
                if (null == (translateActuator = ACTUATORS.get(SqlKind.DELETE))) {
                    translateActuator = new DeleteTranslateActuator(sqlNode, dialect);
                    putTranslate(SqlKind.DELETE,translateActuator);
                }
                return translateActuator;
            case SELECT:
                if (null == (translateActuator = ACTUATORS.get(SqlKind.SELECT))) {
                    translateActuator = new SelectTranslateActuator(sqlNode, dialect);
                    putTranslate(SqlKind.SELECT,translateActuator);
                }
                return translateActuator;
            // 可以继续添加其他 SQL 操作类型的处理
            default:
                throw new UnsupportedOperationException("Unsupported SQL kind: " + sqlNode.getKind());
        }
    }

    public static TranslateActuator getTranslateActuatorOfAuto(String sql, SqlDialect dialect, int mode) throws SqlParseException {
        //todo 未实现
        return null;
    }

}
