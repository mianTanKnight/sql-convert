package com.lishicloud.lsspringbootstartersqlconvert.provide;

import java.util.function.Function;

/**
 * @author admin
 */
public interface SQLConvert {

    /**
     *
     */
    void setConverter(Function<String, String> converter);


}
