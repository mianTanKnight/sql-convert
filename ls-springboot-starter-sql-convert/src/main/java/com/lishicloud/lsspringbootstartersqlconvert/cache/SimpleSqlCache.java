package com.lishicloud.lsspringbootstartersqlconvert.cache;

import com.google.common.collect.Maps;
import com.lishicloud.lsspringbootstartersqlconvert.translate.Pipeline;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * SimpleSqlCache 提供了一个高效且灵活的缓存系统，主要用于缓存 SQL 查询结果。
 * 它特别适用于频繁执行的模板化 SQL 查询，例如 "SELECT * FROM XX WHERE XX.ID = ?"。
 * <p>
 * 该缓存系统的特点包括：
 * 1. 使用软引用存储缓存项，优化内存使用并减轻内存压力。
 * 2. 利用优先级队列管理缓存操作，确保高效处理缓存添加、更新和清理任务。
 * 3. 基于链表的策略动态管理缓存项，支持高效的缓存项访问和淘汰。
 * 4. 自我资源管理，通过智能过期策略和缓冲时间设置，减少频繁的缓存清理操作。
 * 5. 最大化的无锁设计，提高并发处理能力。
 * <p>
 * 此缓存实现适合需要高性能和灵活内存管理的应用场景，尤其适合于数据访问模式具有高度动态性的环境。
 *
 * @author ztq
 */
@Slf4j
public class SimpleSqlCache extends Pipeline {

    // 使用线程安全的 ConcurrentMap 作为缓存存储
    private static Map<String, CacheNode> simpleCache = Maps.newConcurrentMap();

    // 优先级命令队列，用于按优先级顺序处理缓存命令
    private static Queue<LikedCommand> linkedCommands = new PriorityBlockingQueue<>();

    // 单线程链表执行器，确保线程安全并支持优先级作业处理
    private static Thread linkedActuator;

    // 线程运行状态标志
    private static volatile boolean running;

    /**
     * 链表头尾节点，用于快速访问和淘汰缓存项
     * First/Tail 不需要 声明成volatile
     * 因为它们并没有"暴露"出来
     * 而是由单一线程 linkedActuator处理 所以不存在并发问题
     */
    private static CacheNode FIRST = null;
    private static CacheNode TAIL = null;

    /**
     * 强制清理阈值
     * 为了预防长时间的get set 而导致的clear 饥饿
     * 单线程作业是安全的
     */
    private static int mandatoryCleaningThresholdOfDefault = 10000;

    // 缓存参数：过期时间,缓冲时间,时间单位和删除阈值
    private static long expiredTime;
    private static long bufferTime;
    private static int delThreshold;


    /**
     * 构造函数，初始化缓存配置。
     * 设计为 "伪单例"，即整个应用中只应实例化一次。
     */
    public SimpleSqlCache(long expiredTime, TimeUnit expiredTimeUtil, long bufferTime, TimeUnit bufferTimeUtil, int delThreshold) {
        super();
        assert !running;
        SimpleSqlCache.expiredTime = expiredTimeUtil.toMillis(expiredTime);
        SimpleSqlCache.delThreshold = delThreshold;
        SimpleSqlCache.bufferTime = bufferTimeUtil.toMillis(bufferTime);
        running = true;
    }

    /**
     * 添加新的缓存项或更新现有项，并按优先级加入命令到队列。
     */
    public static void put(final  String key, final String v) {
        simpleCache.compute(key, (k, existingValue) -> {
            CacheNode cacheNode = existingValue;
            if (null == cacheNode) {
                cacheNode = new CacheNode(key, new SoftReference<>(v), simpleCache);
            } else {
                cacheNode.updateVal(v);
            }
            pushLinkedCommandsQueue(new LikedCommand(cacheNode, LikedMode.ADD2HEAD));
            unParkAndNotifyClear();
            return cacheNode;
        });
    }


    /**
     * 获取缓存项的值。如果缓存项不存在或已过期，则返回 null。
     */
    @Nullable
    public static String get(final String key) {
        CacheNode cacheNode = simpleCache.computeIfPresent(key, (k, v) -> {
            CacheNode n = null;
            if (!v.expired()) {
                v.addUseSize();
                pushLinkedCommandsQueue(new LikedCommand(v, LikedMode.MOVE2HEAD));
                n = v;
            }
            unParkAndNotifyClear();
            return n;
        });
        return cacheNode != null ? cacheNode.getValue() : null;
    }

    /**
     * 外部手动清理的支持
     */
    public static void unParkAndNotifyClear() {
        if (!registeredOfClearCommand) {
            registeredOfClearCommand = true;
            pushLinkedCommandsQueue(new LikedCommand(null, LikedMode.CLEAR));
        }
        LockSupport.unpark(linkedActuator);
    }

    /**
     * 从缓存中移除指定的键。
     */
    public static void remove(final String key) {
        //safe del
        simpleCache.computeIfPresent(key,(k,v)->{
            v.clear();
            return null;
        });
    }

    /**
     * 向命令队列添加新命令。
     */
    private static void pushLinkedCommandsQueue(LikedCommand clearCommand) {
        linkedCommands.add(clearCommand);
    }


    static {
        linkedActuator = new Thread(() -> {
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    if (linkedCommands.isEmpty()) {
                        LockSupport.park();
                    }
                    LikedCommand command = linkedCommands.poll();
                    if (command != null) {
                        if (log.isDebugEnabled()) {
                            log.info("actuator of command : [{}]", command.getMode());
                        }
                        processCommand(command);
                    }
                } catch (Exception e) {
                    log.error("Error processing commands: {}", e.getMessage(), e);
                }
            }
        });
        linkedActuator.start();
    }

    /**
     * 处理队列中的命令，根据命令类型执行相应操作。
     */
    private static void processCommand(LikedCommand command) {
        CacheNode node = command.getNode();
        if (command.getMode() != LikedMode.CLEAR) {
            if (command.getMode() == LikedMode.ADD2HEAD) {
                node.add2Head();
            } else if (command.getMode() == LikedMode.MOVE2HEAD) {
                node.move2Head();
            }
            if (--mandatoryCleaningThresholdOfDefault <= 0) {
                clearExpiredNodes();
            }
            return;
        }
        clearExpiredNodes();
    }


    private static volatile boolean registeredOfClearCommand = false;




    private static void clearExpiredNodes() {
        /**。
         * 1. 'registeredOfClearCommand作为一个volatile变量，类似于门口的警示灯。它的主要作用是指示清理操作是否正在进行，
         *    而不是强制保证操作的原子性。这样，它可以有效地过滤掉在清理过程中的重复清理命令注册。
         *
         * 2. 移除重复的CLEAR命令：在执行清理操作之前，我们首先检查并移除队列中的重复CLEAR命令，确保清理操作只执行一次。
         *    这是一种有效的无害化并发处理，允许系统在高并发环境下维持较高性能。
         *
         * 3. 清理过期节点：接下来，从TAIL开始清理过期的缓存节点。这一步骤也需注意线程安全和数据一致性。
         *
         * 4. 重置状态：在清理操作完成后，我们重置'mandatoryCleaningThresholdOfDefault'和'registeredOfClearCommand'。
         *    将'registeredOfClearCommand'设置为false是关键，它意味着新的清理命令可以被接受，类似于“重新打开门”。
         */
        while (!linkedCommands.isEmpty() && (linkedCommands.peek()).getMode() == LikedMode.CLEAR) {
            linkedCommands.poll();
        }
        // 从TAIL开始清理过期节点
        CacheNode tailNode;
        while ((tailNode = TAIL) != null && tailNode.expired()) {
            tailNode.clear();
        }
        mandatoryCleaningThresholdOfDefault = 10000;
        registeredOfClearCommand = false;
    }




    @Override
    protected void destroy() {
        synchronized (DESTROY_LOCK) {
            running = false; // 设置标志，指示线程停止运行
            LockSupport.unpark(linkedActuator); // 确保线程从park状态中唤醒
            // 清理资源
            CacheNode cacheNode;
            while (null != (cacheNode = FIRST)) {
                FIRST = FIRST.next;
                cacheNode.clear();
            }
            log.info("SimpleSqlCache Destroy Linked Successful!");
            TAIL = null;
            simpleCache = null;
            linkedCommands = null;
            // 安全地终止线程
            try {
                linkedActuator.join(); // 等待线程安全地结束
            } catch (InterruptedException e) {
                log.error("Error waiting for linkedActuator to finish: {}", e.getMessage(), e);
            }
            linkedActuator = null;
            log.info("SimpleSqlCache Destroy Of All Successful!");
        }
    }

    /**
     * 缓存节点类，代表缓存中的一个项。
     * 包含对应的 SQL 语句的软引用、使用频率统计，以及链表的前后节点引用。
     */
    private static class CacheNode {

        private final String key;  // 缓存项的键
        private SoftReference<String> value;  // 缓存项的值（SQL语句），使用软引用
        private CacheNode prev = null;  // 前一个节点的引用
        private CacheNode next = null;  // 后一个节点的引用
        private int useSize;  // 使用次数统计
        private final long createTime;
        private long lastTime;

        private final Map<String, CacheNode> map;  // 引用缓存的 Map，用于从缓存中移除节点

        /**
         * 构造一个新的缓存节点。
         *
         * @param key   缓存项的键。
         * @param value 缓存项的值。
         * @param map   引用缓存的 Map。
         */
        CacheNode(String key, SoftReference<String> value, Map<String, CacheNode> map) {
            this.key = key;
            this.value = value;
            this.map = map;
            this.useSize = 0;
            lastTime = createTime = System.currentTimeMillis();
        }

        public String getValue() {
            lastTime = System.currentTimeMillis();
            return null != value ? value.get() : null;
        }


        public void updateVal(String val) {
            this.value = new SoftReference<>(val);
        }

        public void addUseSize() {
            this.useSize++;
        }

        /**
         * 将该节点使用头插法添加到链表头部。
         */
        public void add2Head() {
            if (FIRST != null) {
                FIRST.prev = this;
                this.next = FIRST;
            }
            FIRST = this;
            if (TAIL == null) {
                TAIL = this;
            }
        }

        /**
         * 将该节点添加到链表头部。
         */
        public void move2Head() {
            if (this == FIRST) { //已经是first
                return;
            }
            // del this of linked
            removeFromList();
            add2Head();
        }


        public boolean expired() {
            long now = System.currentTimeMillis();
            // 首先检查是否还在缓冲期内
            if (now < createTime + bufferTime) {
                return false; // 如果在缓冲期内，则不算过期
            }
            // 然后检查是否低于最小使用阈值和是否已经过期
            return useSize <= delThreshold || now > (lastTime + expiredTime);
        }


        /**
         * 清除该缓存节点。
         * 从链表中移除并从缓存 Map 中删除该节点。
         */
        public void clear() {
            removeFromList();
            map.remove(this.key);
            if (log.isDebugEnabled()) {
                log.info("Clear Of CacheKey [{}]", this.key);
            }
        }

        private void removeFromList() {
            if (prev != null) {
                prev.next = next;
            }
            if (next != null) {
                next.prev = prev;
            } else {
                // 当前节点是尾节点，更新TAIL指向前一个节点
                TAIL = prev;
            }
            if (this == FIRST) {
                // 当前节点是头节点，更新FIRST
                FIRST = next;
            }
            // 清除当前节点的前后引用
            prev = null;
            next = null;
        }


    }


    /**
     * 优先级命令
     */
    static class LikedCommand implements Comparable<LikedCommand> {
        private final CacheNode node;
        private final int mode;

        LikedCommand(CacheNode node, int mode) {
            this.node = node;
            this.mode = mode;
        }

        public CacheNode getNode() {
            return node;
        }

        public int getMode() {
            return mode;
        }

        @Override
        public int compareTo(LikedCommand other) {
            return Integer.compare(this.mode, other.mode);
        }
    }


    /**
     * 优先级支持
     */
    interface LikedMode {
        int ADD2HEAD = 0;
        int MOVE2HEAD = 1;
        /**
         * CLEAR 并不是一个立即执行的命令 而是类似于.gc()的运作模式
         */
        int CLEAR = 2;
    }
}