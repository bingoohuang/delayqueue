package com.github.bingoohuang.delayqueue;

import redis.clients.jedis.Jedis;

public interface TaskConfig {
    Jedis getJedis();

    String getQueueKey();

    TaskDao getTaskDao();

    String getTaskTableName();

    TaskableFactory getTaskableFactory();
}
