package com.github.bingoohuang.delayqueue.spring;

import com.github.bingoohuang.delayqueue.TaskConfig;
import com.github.bingoohuang.delayqueue.TaskDao;
import com.github.bingoohuang.delayqueue.TaskableFactory;
import org.springframework.beans.factory.annotation.Autowired;
import redis.clients.jedis.Jedis;

public class SpringTaskConfig implements TaskConfig {
    @Autowired Jedis jedis;
    @Autowired TaskDao taskDao;
    @Autowired TaskableFactory factory;

    @Override public Jedis getJedis() {
        return jedis;
    }

    @Override public String getQueueKey() {
        return "delayqueue";
    }

    @Override public TaskDao getTaskDao() {
        return taskDao;
    }

    @Override public String getTaskTableName() {
        return "t_delay_task";
    }

    @Override public TaskableFactory getTaskableFactory() {
        return factory;
    }
}
