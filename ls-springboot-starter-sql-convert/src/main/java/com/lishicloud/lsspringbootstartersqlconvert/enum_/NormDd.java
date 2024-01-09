package com.lishicloud.lsspringbootstartersqlconvert.enum_;


import org.apache.commons.lang3.StringUtils;

/**
 * @author ztq
 */

public enum NormDd {

    //mysql 5.7
    MYSQL("5.7"),

    //达梦 V8
    DM("V8"),

    //common
    COMMON(StringUtils.EMPTY);

    private final String version;

    NormDd(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
