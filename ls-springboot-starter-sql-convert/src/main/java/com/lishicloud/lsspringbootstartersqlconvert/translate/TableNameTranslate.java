package com.lishicloud.lsspringbootstartersqlconvert.translate;


import org.apache.calcite.sql.SqlNode;

/**
 * 针对 SqlKind 是表名的Translate 流水线式处理
 *
 * @author ztq
 */
public abstract class TableNameTranslate extends Pipeline implements Translate {

    /**
     * 领域私有(实现 TableNameTranslate的私有子类共享)
     */
    protected static Translate tableNameNormTranslateOfUnmodifiable = null;

    /**
     * Pipeline of Translate Actuator
     *
     * @param sqlNode 待转换的 SQL 节点。
     * @return 转换后的 SQL 节点。如果未应用任何转换，则返回原始节点。
     */
    public static SqlNode doTranslateOfPipeline(SqlNode sqlNode) {
        return null != tableNameNormTranslateOfUnmodifiable ? tableNameNormTranslateOfUnmodifiable.translate(sqlNode) : sqlNode;
    }

}
