package com.lishicloud.lsspringbootstartersqlconvert.builder.dm;


import com.lishicloud.lsspringbootstartersqlconvert.builder.NormBuilder;
import com.lishicloud.lsspringbootstartersqlconvert.cache.SimpleSqlCache;
import com.lishicloud.lsspringbootstartersqlconvert.provide.ProvideAbility;
import com.lishicloud.lsspringbootstartersqlconvert.provide.SQLConvert;
import com.lishicloud.lsspringbootstartersqlconvert.translate.FunctionTranslate;
import com.lishicloud.lsspringbootstartersqlconvert.translate.Translate;
import com.lishicloud.lsspringbootstartersqlconvert.translate.impl.common.KeyWordTranslator;
import com.lishicloud.lsspringbootstartersqlconvert.translate.impl.dm.DMNorm;
import com.lishicloud.lsspringbootstartersqlconvert.translate.impl.dm.OwnerOfTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 达梦规范 builder
 *
 * @author ztq
 */
public class DMNormTranslateBuilder implements DMNorm, NormBuilder {

    private final List<Translate> normTranslates;

    private SqlDialect sqlDialect;

    private final SQLConvert sqlConvert;
    /**
     * 是否启用SQL缓存
     */
    private boolean enableSqlCache = false;

    public DMNormTranslateBuilder(SQLConvert sqlConvert) {
        assert null != sqlConvert;
        this.normTranslates = new ArrayList<>();
        sqlDialect = new SqlDialect(SqlDialect.EMPTY_CONTEXT
                .withIdentifierQuoteString(""));
        this.sqlConvert = sqlConvert;
    }

    public static DMNormTranslateBuilder builder(SQLConvert sqlConvert) {
        return new DMNormTranslateBuilder(sqlConvert);
    }

    public DMNormTranslateBuilder ownerOfTableDefault(String ownerName) {
        assert StringUtils.isNotBlank(ownerName);
        this.normTranslates.add(new OwnerOfTable(ownerName));
        return this;
    }

    public DMNormTranslateBuilder translateFunctionDefault(FunctionTranslate... functionTranslate) {
        assert null != functionTranslate;
        for (FunctionTranslate translate : functionTranslate) {
            if (translate.valid()) {
                throw new IllegalArgumentException("functionTranslate Must valid");
            }
        }
        return this;
    }

    @Override
    public boolean isEnableSqlCache() {
        return enableSqlCache;
    }

    public DMNormTranslateBuilder enableSqlCache() {
        return enableSqlCache(7, 1, TimeUnit.DAYS, 100);
    }

    public DMNormTranslateBuilder enableSqlCache(long expiredTime, long bufferTime, TimeUnit timeUnit, int delThreshold) {
        this.enableSqlCache = true;
        new SimpleSqlCache(expiredTime, timeUnit, bufferTime, timeUnit, delThreshold);
        return this;
    }

    public DMNormTranslateBuilder updateSqlDialect(SqlDialect sqlDialect) {
        assert null != sqlDialect;
        this.sqlDialect = sqlDialect;
        return this;
    }

    public DMNormTranslateBuilder keyWordTranslatorDefault() {
        return keyWordTranslatorDefault(DEFAULT_EPP, ArrayUtils.EMPTY_STRING_ARRAY);
    }

    public DMNormTranslateBuilder keyWordTranslatorDefault(Function<String, String> epp) {
        assert epp != null;
        return keyWordTranslatorDefault(epp, ArrayUtils.EMPTY_STRING_ARRAY);
    }

    public DMNormTranslateBuilder keyWordTranslatorDefault(String... keyWords) {
        assert ArrayUtils.isNotEmpty(keyWords);
        return keyWordTranslatorDefault(DEFAULT_EPP, keyWords);
    }

    public DMNormTranslateBuilder keyWordTranslatorDefault(Function<String, String> epp, String... keyWords) {
        this.normTranslates.add(new KeyWordTranslator(epp, keyWords));
        return this;
    }

    public final <T extends Translate & DMNorm> void addNormTranslate(List<T> translates) {
        normTranslates.addAll(translates);
    }

    public DMNormTranslateBuilder build() {
        new ProvideAbility(sqlDialect, enableSqlCache, sqlConvert);
        return this;
    }

    @Override
    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }

    @Override
    public List<Translate> getNormTranslates() {
        return normTranslates;
    }

}
