package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.delayqueue.spring.TaskDao;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public interface TaskConfig {
    ZsetCommands getJedis();

    String getQueueKey();

    TaskDao getTaskDao();

    String getTaskTableName();

    Function<String, Taskable> getTaskableFunction();

    Function<String, ResultStoreable> getResultStoreableFunction();

    default ExecutorService getExecutorService() {
        return null;
    }

    /**
     * 版本号逻辑。
     *
     * <pre>
     * 版本号的目的是为了在集群环境部署中，集群节点升级最新版本时，可能导致的问题。
     * 当升级系统版本时，新版本中可能会提交一个新的立即执行的任务，这个新的任务关联的taskService代码在新版本中，
     * 老版本系统可能不存在相关代码，或者代码有缺陷还没有被修正。
     * 如果没有版本号，可能导致老版本节点先获得任务的执行权，然后导致找不到任务的taskService，或者因为代码有缺陷，而执行失败。
     *
     * 如果设定了版本号，则老版本节点在获得任务时，检查到任务的版本号比当前版本的版本号高，则放弃执行。
     * 只有升级了新版本的集群节点才能执行小于等于当前版本号的任务。
     *
     * 版本号，可以使用20181016的格式。
     *
     * 例：NODE1和NODE2当前版本时20181010，现在需要升级到版本20181016。假设先升级NODE1，NODE1升级完后，提交了一个立即执行的任务TASK1,
     * TASK1来自一个更新的版本，自身版本号过低，不适合执行新版本任务，放弃之。只有NODE1获得TASK1任务才能执行。
     * 等NODE2也升级完成后，NODE1和NODE2都升级到了最新的版本号20181010，则无论谁先获得TASK1任务，则都可以执行了。
     * </pre>
     *
     * @return 版本号。
     */
    default long getVersionNumber() {
        return 0L;
    }
}
