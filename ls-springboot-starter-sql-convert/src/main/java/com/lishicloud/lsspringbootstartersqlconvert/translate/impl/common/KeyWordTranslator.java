package com.lishicloud.lsspringbootstartersqlconvert.translate.impl.common;


import com.lishicloud.lsspringbootstartersqlconvert.enum_.NormDd;
import com.lishicloud.lsspringbootstartersqlconvert.translate.ColumnsTranslate;
import com.lishicloud.lsspringbootstartersqlconvert.translate.Norm;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.function.Function;

/**
 *
 * 处理列名中的关键字 转义的实现 支持流水线
 *
 * @author ztq
 */
@Slf4j
public class KeyWordTranslator extends ColumnsTranslate implements Norm {

    private static String SELECT_ALL_KEYWORD = "*";
    private Function<String, String> epp;
    private static Map<String, Void> KEYWORDS_SAFE = Maps.newHashMap();

    private static void PUSH_KEY_WORD(String keyWord) {
        KEYWORDS_SAFE.put(keyWord, null);
    }

    static {
        // 默认关键字添加
        PUSH_KEY_WORD("BY");
        PUSH_KEY_WORD("LIABLE");
        PUSH_KEY_WORD("CHAR");
        PUSH_KEY_WORD("COLUMN");
        PUSH_KEY_WORD("COLUMNS");
        PUSH_KEY_WORD("CURRENT_DATE");
        PUSH_KEY_WORD("CURRENT_TIME");
        PUSH_KEY_WORD("CURRENT_TIMESTAMP");
        PUSH_KEY_WORD("DATE");
        PUSH_KEY_WORD("DATETIME");
        PUSH_KEY_WORD("DESC");
        PUSH_KEY_WORD("KEY");
        PUSH_KEY_WORD("KEYS");
        PUSH_KEY_WORD("READ");
        PUSH_KEY_WORD("TEXT");
    }

    public KeyWordTranslator(Function<String, String> epp) {
        this(epp, ArrayUtils.EMPTY_STRING_ARRAY);
    }

    public KeyWordTranslator(Function<String, String> epp, String... keyWordsOfExternal) {
        this();
        for (String key : keyWordsOfExternal) {
            PUSH_KEY_WORD(key.toUpperCase(Locale.ROOT));
        }
        if (null != epp) {
            this.epp = epp;
        }
    }

    public KeyWordTranslator() {
        super();
        synchronized (REGISTER_LOCK) {
            if (null == selectRowsOfNormTranslateOfUnmodifiable) {
                selectRowsOfNormTranslateOfUnmodifiable = Lists.newArrayList(this);
            } else {
                selectRowsOfNormTranslateOfUnmodifiable.add(this);
            }
        }
    }

    @Override
    public SqlNode translate(SqlNode node) {
        if (node instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) node;
            String rowsName = identifier.getSimple();
            if (!SELECT_ALL_KEYWORD.equals(rowsName) && StringUtils.isNotBlank(rowsName) && KEYWORDS_SAFE.containsKey(rowsName.toUpperCase(Locale.ROOT))) {
                String modifiedName = epp.apply(rowsName);
                return new SqlIdentifier(modifiedName, identifier.getParserPosition());
            }
        }
        return node;
    }

    @Override
    public void destroy() {
        synchronized (DESTROY_LOCK) {
            if (null != selectRowsOfNormTranslateOfUnmodifiable) {
                selectRowsOfNormTranslateOfUnmodifiable.clear();
                KEYWORDS_SAFE = null;
                selectRowsOfNormTranslateOfUnmodifiable = null;
            }
        }
        log.info(this.getClass().getName() + " destroy successful!");
    }

    @Override
    public NormDd fromDb() {
        return NormDd.COMMON;
    }
}
