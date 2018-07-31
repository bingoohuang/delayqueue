package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.delayqueue.spring.TaskDao;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class TaskRunner {
    private final TaskDao taskDao;
    private final String taskTableName;
    private final ZsetCommands zsetCommands;
    private final String queueKey;
    private final Function<String, Taskable> taskableFunction;
    private final Function<String, ResultStoreable> resultStoreFunction;

    @Getter @Setter private volatile boolean loopStopped = false;

    /**
     * 任务运行构造器。
     *
     * @param config 配置
     */
    public TaskRunner(TaskConfig config) {
        this.taskDao = config.getTaskDao();
        this.taskTableName = config.getTaskTableName();
        this.zsetCommands = config.getJedis();
        this.queueKey = config.getQueueKey();
        this.taskableFunction = config.getTaskableFunction();
        this.resultStoreFunction = config.getResultStoreableFunction();
    }

    /**
     * 调用一个异步任务，并且等待其执行，并且返回结果
     *
     * @param taskVo         任务对象
     * @param timeoutSeconds 超时秒数
     * @return 任务对象。（需要调用isInvokeTimeout来判断是否超时）
     */
    public TaskItem invoke(TaskItemVo taskVo, int timeoutSeconds) {
        val taskItem = submit(taskVo);

        val start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start <= timeoutSeconds) {
            TaskUtil.randomSleepMillis(500, 700);

            val task = find(taskItem.getTaskId()).get();
            if (!task.isReadyRun()) return task;
        }

        taskItem.setInvokeTimeout(true);
        return taskItem;
    }

    /**
     * 提交一个异步任务。
     *
     * @param taskVos 任务对象
     * @return 任务对象
     */
    public TaskItem submit(TaskItemVo... taskVos) {
        return submit(Arrays.asList(taskVos)).get(0);
    }

    /**
     * 提交异步任务列表。
     *
     * @param taskVos 任务对象列表
     * @return 任务列表
     */
    public List<TaskItem> submit(List<TaskItemVo> taskVos) {
        val tasks = taskVos.stream()
                .map(TaskItemVo::createTaskItem)
                .collect(Collectors.toList());
        taskDao.add(tasks, taskTableName);
        val map = tasks.stream().collect(
                Collectors.toMap(TaskItem::getTaskId,
                        x -> (double) (x.getRunAt().getMillis())));
        zsetCommands.zadd(queueKey, map);
        return tasks;
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
     * @param classifier  任务分类名称
     * @param reason      取消原因
     * @param relativeIds 关联ID列表
     * @return int 成功取消数量
     */
    public int cancelByRelativeIds(String classifier, String reason, String... relativeIds) {
        List<String> relativeIdList = Lists.newArrayList(relativeIds);
        val tasks = taskDao.queryTaskIdsByRelativeIds(classifier, relativeIdList, taskTableName);
        if (tasks.isEmpty()) return 0;

        return cancel(reason, tasks.stream().map(x -> x.getTaskId()).collect(Collectors.toList()));
    }

    /**
     * 取消一个或多个异步任务.
     *
     * @param reason  取消原因
     * @param taskIds 任务ID列表
     * @return int 成功取消数量
     */
    public int cancel(String reason, List<String> taskIds) {
        zsetCommands.zrem(queueKey, taskIds.toArray(new String[0]));
        return taskDao.cancelTasks(reason, taskIds, TaskItem.待运行, TaskItem.已取消, taskTableName);
    }

    /**
     * 刚启动时，查询所有可以执行的任务，添加到执行列表中。
     *
     * @param classifier 任务分类名称
     */
    public void initialize(String classifier) {
        val tasks = taskDao.listReady(TaskItem.待运行, classifier, taskTableName);
        if (tasks.isEmpty()) return;

        val map = tasks.stream().collect(Collectors.toMap(TaskItem::getTaskId, x -> (double) (x.getRunAt().getMillis())));
        zsetCommands.zadd(queueKey, map);
    }

    /**
     * 循环运行，检查是否有任务，并且运行任务。
     */
    public void run() {
        loopStopped = false;

        while (!loopStopped) {
            if (fire()) continue;
            if (TaskUtil.randomSleepMillis(100, 500))
                break;
        }
    }

    /**
     * 运行一次任务。此方法需要放在循环中调用，或者每秒触发一次，以保证实时性。
     *
     * @return true 成功从队列中抢到一个任务。
     */
    public boolean fire() {
        val taskIds = zsetCommands.zrangeByScore(queueKey, 0, System.currentTimeMillis(), 0, 1);
        if (taskIds.isEmpty()) return false;

        val taskId = taskIds.iterator().next();
        val zrem = zsetCommands.zrem(queueKey, taskId);
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
        TaskItem task = taskDao.find(taskId, taskTableName);
        if (task != null && task.isComplete()) {
            resultStoreFunction.apply(task.getResultStore()).load(task);
        }
        return Optional.ofNullable(task);
    }

    /**
     * 运行任务。
     *
     * @param taskId 任务ID
     */
    public void fire(String taskId) {
        val task = find(taskId);
        if (task.isPresent()) {
            fire(task.get());
        } else {
            log.warn("找不到任务{} ", taskId);
        }
    }

    /**
     * 运行任务。
     *
     * @param task 任务
     */
    public void fire(TaskItem task) {
        task.setStartTime(DateTime.now());
        task.setState(TaskItem.运行中);
        int changed = taskDao.start(task, TaskItem.待运行, taskTableName);
        if (changed == 0) {
            log.debug("任务状态不是待运行{}", task);
            return;
        }

        try {
            val taskable = taskableFunction.apply(task.getTaskService());
            val pair = TaskUtil.timeoutRun(() -> fire(taskable, task), task.getTimeout());
            if (pair._2) {
                log.warn("执行任务超时🌶{}", task);
                endTask(task, TaskItem.已超时, TaskResult.of("任务超时"));
            } else {
                log.info("执行任务成功👌{}", task);
                endTask(task, TaskItem.已完成, pair._1);
            }
        } catch (Exception ex) {
            log.warn("执行任务异常😂{}", task, ex);
            endTask(task, TaskItem.已失败, TaskResult.of(ex.toString()));
        }
    }

    private TaskResult fire(Taskable taskable, TaskItem task) {
        taskable.beforeRun(task);
        try {
            return taskable.run(task);
        } catch (Throwable ex) {
            taskable.afterRun(task, Optional.of(ex));
            throw ex;
        } finally {
            taskable.afterRun(task, Optional.empty());
        }
    }

    private void endTask(TaskItem task, String finalState, TaskResult result) {
        task.setState(finalState);
        task.setResultState(result.getResultState());
        task.setEndTime(DateTime.now());
        resultStoreFunction.apply(task.getResultStore()).store(task, result);
        taskDao.end(task, TaskItem.运行中, taskTableName);
    }
}


