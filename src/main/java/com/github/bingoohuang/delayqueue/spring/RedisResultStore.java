package com.github.bingoohuang.delayqueue.spring;

import com.github.bingoohuang.delayqueue.ResultStoreable;
import com.github.bingoohuang.delayqueue.TaskItem;
import com.github.bingoohuang.delayqueue.TaskResult;
import com.github.bingoohuang.westcache.utils.FastJsons;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCommands;

@Component
public class RedisResultStore implements ResultStoreable {
    @Autowired JedisCommands jedisCommands;

    @Override public void store(TaskItem taskItem, TaskResult taskResult) {
        taskItem.setResultState(taskResult.getResultState());
        String json = FastJsons.json(taskResult.getResult());
        jedisCommands.setex("delayqueue:" + taskItem.getTaskId(), 7 * 24 * 60 * 60, json);

        taskItem.setResult(null);
    }

    @Override public void load(TaskItem taskItem) {
        String json = jedisCommands.get("delayqueue:" + taskItem.getTaskId());
        taskItem.setResult(json);
    }
}
