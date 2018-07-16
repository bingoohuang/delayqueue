package com.github.bingoohuang.delayqueue;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.joda.time.DateTime;
import redis.clients.jedis.JedisCommands;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

@Slf4j
public class TaskRunner implements Runnable {
    private final TaskDao taskDao;
    private final String taskTableName;
    private final JedisCommands jedis;
    private final String queueKey;
    private final TaskableFactory taskableFactory;

    private volatile boolean stop = false;

    /**
     * 任务运行构造器。
     *
     * @param config 配置
     */
    public TaskRunner(TaskConfig config) {
        this.taskDao = config.getTaskDao();
        this.taskTableName = config.getTaskTableName();
        this.jedis = config.getJedis();
        this.queueKey = config.getQueueKey();
        this.taskableFactory = config.getTaskableFactory();
    }

    /**
     * 提交一个异步任务。
     *
     * @param taskVo 任务对象
     * @return 任务对象
     */
    public TaskItem submit(TaskItemVo taskVo) {
        val task = taskVo.createTaskItem();
        taskDao.add(task, taskTableName);
        jedis.zadd(queueKey, taskVo.getRunAt().getMillis(), task.getTaskId());
        return task;
    }

    /**
     * 提交异步任务列表。
     *
     * @param taskVos 任务对象列表
     * @return 任务列表
     */
    public List<TaskItem> submit(List<TaskItemVo> taskVos) {
        val tasks = taskVos.stream().map(TaskItemVo::createTaskItem).collect(Collectors.toList());
        taskDao.add(tasks, taskTableName);
        val map = tasks.stream().collect(toMap(TaskItem::getTaskId, x -> (double) (x.getRunAt().getMillis())));
        jedis.zadd(queueKey, map);
        return tasks;
    }

    /**
     * 取消一个异步任务.
     *
     * @param reason     取消原因
     * @param relativeId 关联ID
     * @return int 成功取消数量
     */
    public int cancelByRelativeId(String reason, String relativeId) {
        return cancelByRelativeId(reason, Lists.newArrayList(relativeId));
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
     * @param reason      取消原因
     * @param relativeIds 关联ID列表
     * @return int 成功取消数量
     */
    public int cancelByRelativeId(String reason, List<String> relativeIds) {
        val tasks = taskDao.queryTaskIdsByRelativeIds(relativeIds, taskTableName);
        val taskIds = tasks.stream().map(x -> x.getTaskId()).collect(Collectors.toList());
        jedis.zrem(queueKey, taskIds.toArray(new String[0]));
        return taskDao.cancelTasks(reason, taskIds, taskTableName);
    }

    /**
     * 取消一个或多个异步任务.
     *
     * @param reason  取消原因
     * @param taskIds 任务ID列表
     * @return int 成功取消数量
     */
    public int cancel(String reason, List<String> taskIds) {
        jedis.zrem(queueKey, taskIds.toArray(new String[0]));
        return taskDao.cancelTasks(reason, taskIds, taskTableName);
    }

    /**
     * 刚启动时，查询所有可以执行的任务，添加到执行列表中。
     */
    public void initialize() {
        val tasks = taskDao.listReady(taskTableName);
        if (tasks.isEmpty()) return;

        val map = tasks.stream().collect(toMap(TaskItem::getTaskId, x -> (double) (x.getRunAt().getMillis())));
        jedis.zadd(queueKey, map);
    }

    /**
     * 循环运行，检查是否有任务，并且运行任务。
     */
    @Override
    public void run() {
        stop = false;

        while (!stop) {
            if (!fire()) {
                Util.randomSleep(500, 1500, TimeUnit.MILLISECONDS);   // 随机休眠0.5秒到1.5秒
            }
        }
    }

    /**
     * 停止循环运行。
     */
    public void stop() {
        stop = true;
    }

    /**
     * 运行一次任务。此方法需要放在循环中调用，或者每秒触发一次，以保证实时性。
     *
     * @return true 成功从队列中抢到一个任务。
     */
    public boolean fire() {
        val taskIds = jedis.zrangeByScore(queueKey, 0, System.currentTimeMillis(), 0, 1);
        if (taskIds.isEmpty()) {
            return false;
        }

        val taskId = taskIds.iterator().next();
        val zrem = jedis.zrem(queueKey, taskId);
        if (zrem < 1) return false; // 该任务已经被其它人抢走了

        fire(taskId);
        return true;
    }


    /**
     * 根据ID查找任务。
     *
     * @param taskId 任务ID
     * @return 找到的任务
     */
    public Optional<TaskItem> find(String taskId) {
        return Optional.ofNullable(taskDao.find(taskId, taskTableName));
    }

    /**
     * 运行任务。
     *
     * @param taskId 任务ID
     */
    public void fire(String taskId) {
        val task = find(taskId);
        if (!task.isPresent()) {
            log.warn("找不到任务{} ", taskId);
            return;
        }

        fire(task.get());
    }

    /**
     * 运行任务。
     *
     * @param task 任务
     */
    public void fire(TaskItem task) {
        task.setStartTime(DateTime.now());
        int changed = taskDao.start(task, taskTableName);
        if (changed == 0) {
            log.debug("任务状态不是待运行{}", task);
            return;
        }

        try {
            val taskable = taskableFactory.getTaskable(task.getTaskService());
            val pair = Util.timeoutRun(() -> taskable.run(task), task.getTimeout());
            if (pair._2) {
                log.warn("执行任务超时🌶{}", task);
                endTask(task, TaskItem.已超时, "任务超时");
            } else {
                log.info("执行任务成功👌{}", task);
                endTask(task, TaskItem.已完成, pair._1);
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
        taskDao.end(task, taskTableName);
    }
}


