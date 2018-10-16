package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.delayqueue.spring.TaskSpringEnabled;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.net.ServerSocket;

@Configuration @ComponentScan @TaskSpringEnabled
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

    @Bean("taskRunner2")
    public TaskRunner taskRunner2(@Autowired SpringTaskConfigVersionNumber config) {
        return new TaskRunner(config);
    }
}