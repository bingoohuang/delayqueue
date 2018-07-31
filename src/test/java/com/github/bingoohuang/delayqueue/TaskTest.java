package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.delayqueue.spring.RedisResultStore;
import com.github.bingoohuang.delayqueue.spring.TaskDao;
import com.github.bingoohuang.westid.WestId;
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
        taskRunner.setLoopStopped(false);
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

        TaskItem item = taskRunner.find("110").get();
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
        TaskItem item = taskRunner.find("120").get();
        assertThat(item.getState()).isEqualTo(TaskItem.已取消);
    }

    @Test
    public void cancelByNonExistingRelativeId() {
        int total = taskRunner.cancelByRelativeIds("default", "手工取消", "xxx");
        assertThat(total).isEqualTo(0);
    }

    @Test
    public void cancelByRelativeId() {
        taskRunner.setLoopStopped(false);
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

        TaskItem item = taskRunner.find("210").get();
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);

        item = taskRunner.find("220").get();
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);
    }

    @Test
    public void timeout() {
        val vo = TaskItemVo.builder().taskId("310").taskName("测试任务").taskService(MyTimeoutTaskable.class.getSimpleName()).timeout(1).build();
        taskRunner.submit(vo);
        taskRunner.fire();

        TaskItem item = taskRunner.find("310").get();
        assertThat(item.getState()).isEqualTo(TaskItem.已超时);
    }

    @Test
    public void timeout2() {
        val vo = TaskItemVo.builder().taskId("320").taskName("测试任务").taskService(MyTimeoutTaskable.class.getSimpleName()).timeout(2).build();
        taskRunner.submit(vo);
        taskRunner.fire();

        TaskItem item = taskRunner.find("320").get();
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);
    }

    @Test
    public void timeout3() {
        val vo = TaskItemVo.builder().taskId("330").taskName("测试任务").taskService(MyExTaskable.class.getSimpleName()).timeout(2).build();
        taskRunner.submit(vo);
        taskRunner.fire();

        TaskItem item = taskRunner.find("330").get();
        assertThat(item.getState()).isEqualTo(TaskItem.已失败);
        assertThat(item.getResultState()).isEqualTo("java.lang.RuntimeException: 😡，竟然崩溃了，泪奔");
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

        TaskUtil.randomSleepMillis(1500, 1800);

        taskRunner.fire();

        TaskItem item = taskRunner.find("410").get();
        assertThat(item.getState()).isEqualTo(TaskItem.已完成);
    }

    @Test
    public void taskException() {
        taskRunner.setLoopStopped(false);

        val vo = TaskItemVo.builder()
                .taskId("510").taskName("测试任务").taskService(MyExTaskable.class.getSimpleName())
                .build();
        taskRunner.submit(vo);

        taskRunner.fire();

        TaskItem item = taskRunner.find("510").get();
        assertThat(item.getState()).isEqualTo(TaskItem.已失败);
        assertThat(item.getResultState()).isEqualTo("java.lang.RuntimeException: 😡，竟然崩溃了，泪奔");
    }

    @Test
    public void run() {
        Executors.newSingleThreadExecutor().submit(() -> taskRunner.run());
        TaskUtil.randomSleepMillis(100, 200);
        taskRunner.setLoopStopped(true);

        assertThat(taskRunner.isLoopStopped()).isTrue();
    }

    @Test
    public void invokeDirect() {
        taskRunner.setLoopStopped(false);
        String taskId = String.valueOf(WestId.next());
        val vo = TaskItemVo.builder()
                .taskId(taskId).taskName("测试任务").taskService(MyInvokeTaskable.class.getSimpleName())
                .build();

        taskRunner.submit(vo);
        taskRunner.fire();
        TaskItem taskItem = taskRunner.find(taskId).get();
        assertThat(taskItem.getResultAsString()).isEqualTo("DANGDANGDANG");
    }

    @Test
    public void invokeRedis() {
        Executors.newSingleThreadExecutor().submit(() -> taskRunner.run());

        String taskId1 = String.valueOf(WestId.next());
        val vo1 = TaskItemVo.builder()
                .taskId(taskId1).taskName("测试任务").taskService(MyInvokeTaskable.class.getSimpleName())
                .resultStore(RedisResultStore.class.getSimpleName())
                .build();

        TaskItem item1 = taskRunner.invoke(vo1, 3000);
        assertThat(item1.getResultAsString()).isEqualTo("DANGDANGDANG");


        String taskId2 = String.valueOf(WestId.next());
        val vo2 = TaskItemVo.builder()
                .taskId(taskId2).taskName("测试任务").taskService(MyInvokeTaskable.class.getSimpleName())
                .resultStore(RedisResultStore.class.getSimpleName())
                .build();

        TaskItem item2 = taskRunner.invoke(vo2, -1);
        assertThat(item2.isInvokeTimeout()).isEqualTo(true);

        taskRunner.setLoopStopped(true);
    }
}
