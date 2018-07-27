package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.delayqueue.spring.TaskDao;
import redis.clients.jedis.JedisCommands;

public interface TaskConfig {
    JedisCommands getJedis();

    String getQueueKey();

    TaskDao getTaskDao();

    String getTaskTableName();

    TaskableFactory getTaskableFactory();
}
