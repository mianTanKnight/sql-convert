package com.lishicloud.lsspringbootstartersqlconvert.builder;

import com.lishicloud.lsspringbootstartersqlconvert.translate.Translate;
import org.apache.calcite.sql.SqlDialect;

import java.util.List;

/**
 * @author ztq
 */
public interface NormBuilder {


    /**
     *
     */
    SqlDialect getSqlDialect();


    /**
     *
     */
    List<Translate> getNormTranslates();


    /**
     *
     */
    boolean isEnableSqlCache();

}
