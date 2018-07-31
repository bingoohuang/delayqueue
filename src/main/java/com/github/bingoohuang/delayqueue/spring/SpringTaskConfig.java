package com.github.bingoohuang.delayqueue.spring;

import com.github.bingoohuang.delayqueue.TaskConfig;
import com.github.bingoohuang.delayqueue.Taskable;
import com.github.bingoohuang.delayqueue.ZsetCommands;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCommands;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

@Component
public class SpringTaskConfig implements TaskConfig {
    @Autowired JedisCommands jedis;
    @Getter @Autowired TaskDao taskDao;
    @Autowired SpringTaskableFactory factory;
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
        return new ZsetCommands() {
            @Override public Long zadd(String key, Map<String, Double> scoreMembers) {
                return jedis.zadd(key, scoreMembers);
            }

            @Override public Long zrem(String key, String... member) {
                return jedis.zrem(key, member);
            }

            @Override public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
                return jedis.zrangeByScore(key, min, max, offset, count);
            }
        };
    }

    @Override public Function<String, Taskable> getTaskableFunction() {
        return factory::getTaskService;
    }
}
