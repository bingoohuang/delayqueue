package com.github.bingoohuang.delayqueue;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.joda.time.DateTime;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

@Slf4j
public class TaskService implements Runnable {
    private final TaskConfig config;
    private volatile boolean stop = false;

    public TaskService(TaskConfig config) {
        this.config = config;
    }

    /**
     * 提交一个异步任务。
     *
     * @param task 任务对象
     */
    public void submit(TaskItemVo task) {
        config.getTaskDao().add(task.createTaskItem(), config.getTaskTableName());
        config.getJedis().zadd(config.getQueueKey(), task.getReadyTime().getMillis(), task.getTaskId());
    }

    /**
     * 提交异步任务列表。
     *
     * @param tasks 任务对象列表
     */
    public void submit(List<TaskItemVo> tasks) {
        config.getTaskDao().add(tasks.stream().map(TaskItemVo::createTaskItem).collect(Collectors.toList()), config.getTaskTableName());
        val map = tasks.stream().collect(toMap(TaskItemVo::getTaskId, x -> (double) (x.getReadyTime().getMillis())));
        config.getJedis().zadd(config.getQueueKey(), map);
    }

    /**
     * 取消一个异步任务.
     *
     * @param reason 取消原因
     * @param taskId 任务ID
     * @return int 成功取消数量
     */
    public int cancel(String reason, String taskId) {
        return cancel(reason, Lists.newArrayList(taskId));
    }

    /**
     * 取消一个或多个异步任务.
     *
     * @param reason  取消原因
     * @param taskIds 任务ID列表
     * @return int 成功取消数量
     */
    public int cancel(String reason, List<String> taskIds) {
        val taskIdArr = taskIds.toArray(new String[0]);
        config.getJedis().zrem(config.getQueueKey(), taskIdArr);
        return config.getTaskDao().cancelTasks(config.getTaskTableName(), reason, taskIdArr);
    }

    /**
     * 刚启动时，查询所有可以执行的任务
     */
    public void initialize() {
        val tasks = config.getTaskDao().listReady(config.getTaskTableName());
        if (tasks.isEmpty()) return;

        val map = tasks.stream().collect(toMap(TaskItem::getTaskId, x -> (double) (x.getReadyTime().getMillis())));
        config.getJedis().zadd(config.getQueueKey(), map);
    }

    /**
     * 循环运行，检查是否有任务，并且运行任务。
     */
    @Override
    public void run() {
        stop = false;

        while (!stop) {
            fire();
        }
    }

    public void stop() {
        stop = true;
    }

    /**
     * 运行一次任务。此方法需要放在循环中调用，或者每秒触发一次，以保证实时性。
     *
     * @return redis触发列表是否为空
     */
    public void fire() {
        val taskIds = config.getJedis().zrangeByScore(config.getQueueKey(), 0, System.currentTimeMillis());

        if (taskIds.isEmpty()) {
            Util.randomSleep(500, 1500, TimeUnit.MILLISECONDS);   // 随机休眠0.5秒到1.5秒
            return;
        }

        val taskId = taskIds.iterator().next();
        val zrem = config.getJedis().zrem(config.getQueueKey(), taskId);
        if (zrem < 1) return; // 该任务已经被其它人抢走了

        fire(taskId);
    }


    public TaskItem find(String taskId) {
        return config.getTaskDao().find(taskId, config.getTaskTableName());
    }

    public void fire(String taskId) {
        val task = find(taskId);
        if (task == null) {
            log.warn("找不到任务{} ", taskId);
            return;
        }

        fire(task);
    }

    public void fire(TaskItem task) {
        task.setStartTime(DateTime.now());
        int changed = config.getTaskDao().start(task, config.getTaskTableName());
        if (changed == 0) {
            log.debug("任务 {} {} 状态不是待运行", task.getTaskId(), task.getTaskName());
            return;
        }

        try {
            val taskable = config.getTaskableFactory().getTaskable(task.getTaskService());
            if (Util.timeoutRun(() -> taskable.run(task), task.getTimeout())) {
                log.warn("执行任务超时🌶{}", task);
                endTask(task, TaskItem.已超时, "任务超时");
            } else {
                log.info("执行任务成功👌{}", task);
                endTask(task, TaskItem.已完成, "执行成功");
            }
        } catch (Exception ex) {
            log.warn("执行任务异常😂{}", task, ex);
            endTask(task, TaskItem.已失败, ex.toString());
        }
    }

    private void endTask(TaskItem task, String finalState, String result) {
        task.setState(finalState);
        task.setResult(result);
        task.setEndTime(DateTime.now());
        config.getTaskDao().end(task, config.getTaskTableName());
    }
}


