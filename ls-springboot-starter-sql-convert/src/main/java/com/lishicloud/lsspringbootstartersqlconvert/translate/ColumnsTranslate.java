package com.lishicloud.lsspringbootstartersqlconvert.translate;

import org.apache.calcite.sql.SqlNode;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * 针对 Select Or Having `s columns 的Translate 流水线式处理
 * @author ztq
 */
public abstract class ColumnsTranslate extends Pipeline implements Translate {

    /**
     * 领域私有(实现 SelectRowsTranslate)
     */
    protected static List<ColumnsTranslate> selectRowsOfNormTranslateOfUnmodifiable = null;


    /**
     * Pipeline of Translate Actuator
     *
     * @param sqlNode 待转换的 SQL 节点。
     * @return 转换后的 SQL 节点。如果未应用任何转换，则返回原始节点。
     */
    public static SqlNode doTranslateOfPipeline(SqlNode sqlNode) {
        SqlNode targetNode = sqlNode;
        if (!CollectionUtils.isEmpty(selectRowsOfNormTranslateOfUnmodifiable)) {
            for (Translate translateObj : selectRowsOfNormTranslateOfUnmodifiable) {
                targetNode =translateObj.translate(targetNode);
            }
        }
        return targetNode;
    }

}
