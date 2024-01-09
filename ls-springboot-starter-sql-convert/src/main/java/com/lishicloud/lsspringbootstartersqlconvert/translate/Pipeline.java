package com.lishicloud.lsspringbootstartersqlconvert.translate;


import com.google.common.collect.Lists;

import java.util.List;

/**
 * 流水线的抽象定义。支持资源的自动化管理
 * 用于表示一系列操作的集合，这些操作按照特定的顺序应用于数据或对象。
 * n = 1 也是属于流水线作业
 * @author ztq
 */
public abstract class Pipeline {

    protected static List<Pipeline> DESTROY_LIST = Lists.newLinkedList();

    public Pipeline() {
        DESTROY_LIST.add(this);
    }

    /**
     * 所有Translate实现安全注销的锁
     */
    protected static final Object DESTROY_LOCK = new Object();

    static {
        // 注册关闭时的钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                for (Pipeline pipeline : DESTROY_LIST) {
                    pipeline.destroy();
                }
                DESTROY_LIST = null;
        }));
    }

    /**
     * 销毁流水线，进行必要的清理工作。
     * 当流水线不再需要时，或者在应用程序关闭时调用。
     */
    protected abstract void destroy();
}
