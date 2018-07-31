package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.delayqueue.spring.TaskDao;

import java.util.function.Function;

public interface TaskConfig {
    ZsetCommands getJedis();

    String getQueueKey();

    TaskDao getTaskDao();

    String getTaskTableName();

    Function<String, Taskable> getTaskableFunction();
}
