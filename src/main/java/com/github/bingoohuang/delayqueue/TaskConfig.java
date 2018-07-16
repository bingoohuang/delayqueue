package com.github.bingoohuang.delayqueue;

import redis.clients.jedis.JedisCommands;

public interface TaskConfig {
    JedisCommands getJedis();

    String getQueueKey();

    TaskDao getTaskDao();

    String getTaskTableName();

    TaskableFactory getTaskableFactory();
}
