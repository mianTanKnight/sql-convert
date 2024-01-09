package com.lishicloud.lsspringbootstartersqlconvert.translate;

import org.apache.calcite.sql.SqlNode;


/**
 * SQL语法树（AST）转换的顶层接口。
 * 用于定义如何转换SQL节点，特别是在不同数据库方言间转换时。
 * @author ztq
 */
public interface Translate {

    /**
     * 所有Translate实现安全注册的锁
     */
    Object REGISTER_LOCK = new Object();

    /**
     * 转换指定的 SQL 节点。
     * 实现此方法以提供节点的具体转换逻辑。
     *
     * @param node 待转换的 SQL 节点
     * @return 转换后的 SQL 节点
     */
    SqlNode translate(SqlNode node);


}
