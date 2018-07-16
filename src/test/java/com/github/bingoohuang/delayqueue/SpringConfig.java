package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.delayqueue.spring.SpringTaskConfig;
import com.github.bingoohuang.delayqueue.spring.SpringTaskableFactory;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.n3r.eql.eqler.EqlerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.net.ServerSocket;

@Configuration @ComponentScan
public class SpringConfig {
    @SneakyThrows
    public static int getRandomPort() {
        @Cleanup val socket = new ServerSocket(0);
        return socket.getLocalPort();
    }

    @Bean
    public Jedis jedis() {
        int port = getRandomPort();
        val redis1 = new RedisServer(port);
        redis1.start();

        return new Jedis("127.0.0.1", port);
    }

    @Bean
    public TaskableFactory taskableFactory() {
        return new SpringTaskableFactory();
    }

    @Bean
    public TaskConfig taskConfig() {
        return new SpringTaskConfig();
    }

    @Bean
    public TaskDao taskDao() {
        return EqlerFactory.getEqler(TaskDao.class);
    }

    @Bean TaskRunner taskService(@Autowired TaskConfig taskConfig) {
        return new TaskRunner(taskConfig);
    }
}