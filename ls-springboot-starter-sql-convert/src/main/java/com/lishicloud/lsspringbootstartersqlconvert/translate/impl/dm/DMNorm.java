package com.lishicloud.lsspringbootstartersqlconvert.translate.impl.dm;

import com.lishicloud.lsspringbootstartersqlconvert.enum_.NormDd;
import com.lishicloud.lsspringbootstartersqlconvert.translate.Norm;

import java.util.function.Function;

/**
 * 达梦规范
 * @author admin
 */
public interface DMNorm extends Norm {

    /**
     * 达梦
     */
    @Override
    default NormDd fromDb() {
        return NormDd.DM;
    }

    /**
     * 默认关键字转义规则
     */
    Function<String, String> DEFAULT_EPP = content -> "'" + content + "'";


}
