package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.delayqueue.spring.TaskDao;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
}
