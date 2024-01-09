package com.lishicloud.lsspringbootstartersqlconvert.provide;


import com.lishicloud.lsspringbootstartersqlconvert.cache.SimpleSqlCache;
import com.lishicloud.lsspringbootstartersqlconvert.factory.SqlTranslateActuatorFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang3.StringUtils;


/**
 * @author admin
 */
@Slf4j
public class ProvideAbility  {

    private final SqlDialect sqlDialect;
    private final boolean enableSqlCache;

    public ProvideAbility(SqlDialect sqlDialect, boolean enableSqlCache, SQLConvert sqlConvert) {
        this.sqlDialect = sqlDialect;
        this.enableSqlCache = enableSqlCache;
        sqlConvert.setConverter(this::convert);
    }

    public String convert(String originalSQL) {
        log.info("SQLDialect --> original sql : [{}]", originalSQL);
        String newSQL;
        if (!enableSqlCache || StringUtils.isBlank(newSQL = SimpleSqlCache.get(originalSQL))) {
            try {
                newSQL = SqlTranslateActuatorFactory.getTranslateActuatorOfAuto(originalSQL, sqlDialect).translateActuatorSql();
            } catch (SqlParseException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getCause().getMessage());
            }
            SimpleSqlCache.put(originalSQL, newSQL);
            log.info("SQLDialect --> after processing sql, From TranslateActuator : [{}]", newSQL);
        } else {
            log.info("SQLDialect --> after processing sql, From Cache : [{}]", newSQL);
        }
        return newSQL;
    }
}

