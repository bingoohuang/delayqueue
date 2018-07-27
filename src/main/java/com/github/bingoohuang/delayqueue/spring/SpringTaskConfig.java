package com.github.bingoohuang.delayqueue.spring;

import com.github.bingoohuang.delayqueue.TaskConfig;
import com.github.bingoohuang.delayqueue.TaskableFactory;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCommands;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Component
public class SpringTaskConfig implements TaskConfig {
    @Autowired JedisCommands jedis;
    @Autowired TaskDao taskDao;
    @Autowired TaskableFactory factory;
    private String queueKey;
    private String taskTableName;

    @PostConstruct @SneakyThrows
    public void postContruct() {
        @Cleanup val is = SpringTaskConfig.class.getClassLoader().getResourceAsStream("delayqueue.properties");

        val p = new Properties();
        if (is != null) p.load(is);

        this.queueKey = p.getProperty("QueueKey", "delayqueue");
        this.taskTableName = p.getProperty("TaskTableName", "t_delay_task");
    }

    @Override public JedisCommands getJedis() {
        return jedis;
    }

    @Override public String getQueueKey() {
        return queueKey;
    }

    @Override public TaskDao getTaskDao() {
        return taskDao;
    }

    @Override public String getTaskTableName() {
        return taskTableName;
    }

    @Override public TaskableFactory getTaskableFactory() {
        return factory;
    }
}
