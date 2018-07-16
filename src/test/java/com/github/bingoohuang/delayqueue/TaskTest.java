package com.github.bingoohuang.delayqueue;

import com.google.common.collect.Lists;
import lombok.val;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.n3r.eql.Eql;
import org.n3r.eql.util.C;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import redis.clients.jedis.Jedis;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SpringConfig.class})
public class TaskTest {
    @Autowired TaskService taskService;
    @Autowired Jedis jedis;
    @Autowired TaskConfig taskConfig;
    @Autowired TaskDao taskDao;

    @BeforeClass
    public static void beforeClass() {
        String sql = C.classResourceToString("h2-createTable.sql");
        new Eql().execute(sql);

    }

    @Test
    public void submit() {
        taskService.initialize();

        val vo = TaskItemVo.builder()
                .taskId("110").taskName("测试任务").taskService("MyTaskable")
                .relativeId("关联ID")
                .build();
        taskService.submit(vo);

        taskService.initialize();

        Set<String> set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).containsExactly("110");

        taskService.fire();
        set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).isEmpty();

        TaskItem item = taskDao.find("110", taskConfig.getTaskTableName());
        assertThat(item.getTaskId()).isEqualTo("110");
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);

        taskService.fire(item);
        taskService.fire(item.getTaskId());
        taskService.fire("not exists");
    }

    @Test
    public void cancel() {
        val vo = TaskItemVo.builder().taskId("120").taskName("测试任务").taskService("MyTaskable").build();
        taskService.submit(vo);

        Set<String> set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).containsExactly("120");

        taskService.cancel("手工取消", "120");

        set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).isEmpty();
        TaskItem item = taskDao.find("120", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已取消);
    }


    @Test
    public void submitMulti() {
        val vo1 = TaskItemVo.builder().taskId("210").taskName("测试任务").taskService("MyTaskable").build();
        val vo2 = TaskItemVo.builder().taskId("220").taskName("测试任务").taskService("MyTaskable").build();
        taskService.submit(Lists.newArrayList(vo1, vo2));

        Set<String> set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).containsExactly("210", "220");

        taskService.fire();
        set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).hasSize(1);

        taskService.fire();
        set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).hasSize(0);

        TaskItem item = taskDao.find("210", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);

        item = taskDao.find("220", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);
    }

    @Test
    public void timeout() {
        val vo = TaskItemVo.builder().taskId("310").taskName("测试任务").taskService("MyTimeoutTaskable").timeout(1).build();
        taskService.submit(vo);
        taskService.fire();

        TaskItem item = taskDao.find("310", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已超时);
    }

    @Test
    public void timeout2() {
        val vo = TaskItemVo.builder().taskId("320").taskName("测试任务").taskService("MyTimeoutTaskable").timeout(2).build();
        taskService.submit(vo);
        taskService.fire();

        TaskItem item = taskDao.find("320", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);
    }

    @Test
    public void timeout3() {
        val vo = TaskItemVo.builder().taskId("330").taskName("测试任务").taskService("MyExTaskable").timeout(2).build();
        taskService.submit(vo);
        taskService.fire();

        TaskItem item = taskDao.find("330", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已失败);
        assertThat(item.getResult()).isEqualTo("java.lang.RuntimeException: 😡，竟然崩溃了，泪奔");
    }

    @Test(expected = RuntimeException.class)
    public void taskNameRequired() {
        taskService.submit(TaskItemVo.builder().taskService("MyTaskable").build());
    }

    @Test(expected = RuntimeException.class)
    public void taskServiceRequired() {
        taskService.submit(TaskItemVo.builder().taskName("MyTaskable").build());
    }


    @Test
    public void delay() {
        jedis.del(taskConfig.getQueueKey());
        taskService.fire();

        val vo = TaskItemVo.builder()
                .taskId("410").taskName("测试任务").taskService("MyTaskable")
                .readyTime(DateTime.now().plusMillis(1500))
                .build();
        taskService.submit(vo);
        taskService.fire();

        Set<String> set = jedis.zrangeByScore(taskConfig.getQueueKey(), 0, System.currentTimeMillis());
        assertThat(set).isEmpty();

        Util.randomSleep(1500, 1600, TimeUnit.MILLISECONDS);

        taskService.fire();
        set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).isEmpty();

        TaskItem item = taskDao.find("410", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);
    }

    @Test
    public void taskException() {
        val vo = TaskItemVo.builder()
                .taskId("510").taskName("测试任务").taskService("MyExTaskable")
                .build();
        taskService.submit(vo);

        taskService.fire();

        TaskItem item = taskDao.find("510", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已失败);
        assertThat(item.getResult()).isEqualTo("java.lang.RuntimeException: 😡，竟然崩溃了，泪奔");
    }


    @Test
    public void run() {
        Executors.newSingleThreadExecutor().submit(taskService);
        Util.randomSleep(100, 200, TimeUnit.MILLISECONDS);
        taskService.stop();
    }
}
