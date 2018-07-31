package com.github.bingoohuang.delayqueue.spring;

import com.github.bingoohuang.delayqueue.*;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCommands;

import javax.annotation.PostConstruct;
import java.util.Properties;
import java.util.function.Function;

@Component
public class SpringTaskConfig implements TaskConfig {
    @Autowired JedisCommands jedis;
    @Getter @Autowired TaskDao taskDao;
    @Autowired SpringBeanFactory factory;
    @Getter private String queueKey;
    @Getter private String taskTableName;

    @PostConstruct @SneakyThrows
    public void postConstruct() {
        val classLoader = SpringTaskConfig.class.getClassLoader();
        @Cleanup val is = classLoader.getResourceAsStream("delayqueue.properties");

        val p = new Properties();
        if (is != null) p.load(is);

        this.queueKey = p.getProperty("QueueKey", "delayqueue");
        this.taskTableName = p.getProperty("TaskTableName", "t_delay_task");
    }

    @Override public ZsetCommands getJedis() {
        return DelayQueueUtil.adapt(jedis);
    }

    @Override public Function<String, Taskable> getTaskableFunction() {
        return factory::getBean;
    }

    @Override public Function<String, ResultStoreable> getResultStoreableFunction() {
        return factory::getBean;
    }
}
