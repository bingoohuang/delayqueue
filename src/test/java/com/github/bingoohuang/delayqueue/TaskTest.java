package com.github.bingoohuang.delayqueue;

import com.google.common.collect.Lists;
import lombok.val;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
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
    @Autowired TaskRunner taskRunner;
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
        taskRunner.initialize("default");

        val attachment = AttachmentVo.builder().name("黄进兵").age(110)
                .createTime(DateTime.parse("2018-07-19 11:02:17", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")))
                .build();
        val vo = TaskItemVo.builder()
                .taskId("110").taskName("测试任务").taskService(MyTaskable.class.getSimpleName())
                .relativeId("关联ID")
                .attachment(attachment)
                .build();
        val task = taskRunner.submit(vo);

        taskRunner.initialize("default");

        Set<String> set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).contains(task.getTaskId());

        taskRunner.fire();
        set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).isEmpty();

        TaskItem item = taskDao.find("110", taskConfig.getTaskTableName());
        assertThat(item.getTaskId()).isEqualTo("110");
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);
        assertThat(item.getAttachmentAsString()).isEqualTo("{\"createTime\":\"2018-07-19 11:02:17.000\",\"name\":\"黄进兵\",\"age\":110}");
        assertThat(item.getAttachment(AttachmentVo.class)).isEqualTo(attachment);

        taskRunner.fire(item);
        taskRunner.fire(item.getTaskId());
        taskRunner.fire("not exists");
    }

    @Test
    public void cancel() {
        val vo = TaskItemVo.builder().taskId("120").taskName("测试任务").taskService(MyTaskable.class.getSimpleName()).build();
        taskRunner.submit(vo);

        Set<String> set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).containsExactly("120");

        taskRunner.cancel("手工取消", "120");

        set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).isEmpty();
        TaskItem item = taskDao.find("120", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已取消);
    }

    @Test
    public void cancelByNonExistingRelativeId() {
        int total = taskRunner.cancelByRelativeIds("default", "手工取消", "xxx");
        assertThat(total).isEqualTo(0);
    }

    @Test
    public void cancelByRelativeId() {
        val vo = TaskItemVo.builder().relativeId("120").taskName("测试任务").taskService(MyTaskable.class.getSimpleName()).build();
        val task = taskRunner.submit(vo);

        Set<String> set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).contains(task.getTaskId());

        taskRunner.cancelByRelativeIds("default", "手工取消", "120");

        set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).isEmpty();
        val items = taskDao.queryTaskIdsByRelativeIds("default", Lists.newArrayList("120"), taskConfig.getTaskTableName());
        assertThat(items).hasSize(1);
        assertThat(items.get(0).getState()).isEqualTo(TaskItem.已取消);
    }


    @Test
    public void submitMulti() {
        val vo1 = TaskItemVo.builder().taskId("210").taskName("测试任务").taskService(MyTaskable.class.getSimpleName()).build();
        val vo2 = TaskItemVo.builder().taskId("220").taskName("测试任务").taskService(MyTaskable.class.getSimpleName()).build();
        taskRunner.submit(Lists.newArrayList(vo1, vo2));

        Set<String> set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).containsExactly("210", "220");

        taskRunner.fire();
        set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).hasSize(1);

        taskRunner.fire();
        set = jedis.zrange(taskConfig.getQueueKey(), 0, -1);
        assertThat(set).hasSize(0);

        TaskItem item = taskDao.find("210", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);

        item = taskDao.find("220", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);
    }

    @Test
    public void timeout() {
        val vo = TaskItemVo.builder().taskId("310").taskName("测试任务").taskService(MyTimeoutTaskable.class.getSimpleName()).timeout(1).build();
        taskRunner.submit(vo);
        taskRunner.fire();

        TaskItem item = taskDao.find("310", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已超时);
    }

    @Test
    public void timeout2() {
        val vo = TaskItemVo.builder().taskId("320").taskName("测试任务").taskService(MyTimeoutTaskable.class.getSimpleName()).timeout(2).build();
        taskRunner.submit(vo);
        taskRunner.fire();

        TaskItem item = taskDao.find("320", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);
    }

    @Test
    public void timeout3() {
        val vo = TaskItemVo.builder().taskId("330").taskName("测试任务").taskService(MyExTaskable.class.getSimpleName()).timeout(2).build();
        taskRunner.submit(vo);
        taskRunner.fire();

        TaskItem item = taskDao.find("330", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已失败);
        assertThat(item.getResult()).isEqualTo("java.lang.RuntimeException: 😡，竟然崩溃了，泪奔");
    }

    @Test(expected = RuntimeException.class)
    public void taskNameRequired() {
        taskRunner.submit(TaskItemVo.builder().taskService(MyTaskable.class.getSimpleName()).build());
    }

    @Test(expected = RuntimeException.class)
    public void taskServiceRequired() {
        taskRunner.submit(TaskItemVo.builder().taskName(MyTaskable.class.getSimpleName()).build());
    }

    @Test
    public void delay() {
        jedis.del(taskConfig.getQueueKey());
        taskRunner.fire();

        val vo = TaskItemVo.builder()
                .taskId("410").taskName("测试任务").taskService(MyTaskable.class.getSimpleName())
                .runAt(DateTime.now().plusMillis(1000))
                .build();
        taskRunner.submit(vo);
        taskRunner.fire();

        Set<String> set = jedis.zrangeByScore(taskConfig.getQueueKey(), 0, System.currentTimeMillis());
        assertThat(set).isEmpty();

        Util.randomSleep(1500, 1800, TimeUnit.MILLISECONDS);

        taskRunner.fire();

        TaskItem item = taskDao.find("410", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);
    }

    @Test
    public void taskException() {
        val vo = TaskItemVo.builder()
                .taskId("510").taskName("测试任务").taskService(MyExTaskable.class.getSimpleName())
                .build();
        taskRunner.submit(vo);

        taskRunner.fire();

        TaskItem item = taskDao.find("510", taskConfig.getTaskTableName());
        assertThat(item.getState()).isEqualTo(TaskItem.已失败);
        assertThat(item.getResult()).isEqualTo("java.lang.RuntimeException: 😡，竟然崩溃了，泪奔");
    }


    @Test
    public void run() {
        Executors.newSingleThreadExecutor().submit(taskRunner);
        Util.randomSleep(100, 200, TimeUnit.MILLISECONDS);
        taskRunner.setLoopStopped(true);

        assertThat(taskRunner.isLoopStopped()).isTrue();
    }
}
