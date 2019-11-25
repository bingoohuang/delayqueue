package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.delayqueue.spring.TaskDao;
import com.github.bingoohuang.utils.cron.CronAlias;
import com.github.bingoohuang.utils.lang.Mapp;
import com.github.bingoohuang.utils.lang.Threadx;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.of;
import static org.joda.time.DateTime.now;

@Slf4j
public class TaskRunner {
  private static final String VERSION_NUMBER_SEP = "@";
  private final TaskDao taskDao;
  private final String taskTableName;
  private final ZsetCommands zsetCommands;
  private final Function<Long, String> queueKey;
  private final Function<String, Taskable> taskableFunction;
  private final Function<String, ResultStoreable> resultStoreFunction;
  private final ExecutorService executorService;
  private final long versionNumber;
  private final int lastRunMinutesAgo;

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
    this.executorService = config.getExecutorService();
    this.versionNumber = config.getVersionNumber();
    this.lastRunMinutesAgo = config.getLastRunMinutesAgo();
  }

  /**
   * 调用一个异步任务，并且等待其执行，并且返回结果
   *
   * @param taskVo 任务对象
   * @param timeoutSeconds 超时秒数
   * @return 任务对象。（需要调用isInvokeTimeout来判断是否超时）
   */
  public TaskItem invoke(TaskItemVo taskVo, int timeoutSeconds) {
    val taskItem = submit(taskVo);

    val start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start <= timeoutSeconds) {
      Threadx.randomSleepMillis(500, 700);

      val task = find(taskItem.getTaskId());
      if (task.isPresent() && !task.get().isReadyRun()) return task.get();
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
    val tasks =
        taskVos.stream().map(x -> x.createTaskItem(versionNumber)).collect(Collectors.toList());
    return submitTasks(tasks);
  }

  private List<TaskItem> submitTasks(List<TaskItem> tasks) {
    taskDao.add(tasks, taskTableName);
    return triggerTask(tasks);
  }

  /**
   * 调整任务，包括调整任务执行时间，或者任务其它属性。
   *
   * @param taskItem 任务。
   */
  public void adjustTask(TaskItem taskItem) {
    val oldTask = find(taskItem.getTaskId());
    if (!oldTask.isPresent()) {
      submitTasks(Lists.newArrayList(taskItem));
      return;
    }

    taskDao.updateTask(taskItem, taskTableName);
    if (!oldTask.get().getRunAt().equals(taskItem.getRunAt())
        && TaskItem.待运行.equals(taskItem.getState())) {
      val key = createTaskIdWithVersionNumber(taskItem);
      val millis = taskItem.getRunAt().getMillis();
      val map = Mapp.of(key, (double) millis);
      zsetCommands.zadd(queueKey.apply(millis), map);
    }
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
   * @param classifier 任务分类名称
   * @param reason 取消原因
   * @param relativeIds 关联ID列表
   * @return int 成功取消数量
   */
  public int cancelByRelativeIds(String classifier, String reason, String... relativeIds) {
    List<String> relativeIdList = Lists.newArrayList(relativeIds);
    val tasks = queryTasksByRelativeIds(classifier, relativeIdList);
    if (tasks.isEmpty()) return 0;

    return cancel(reason, tasks.stream().map(TaskItem::getTaskId).collect(Collectors.toList()));
  }

  /**
   * 根据关联ID查询任务列表。
   *
   * @param classifier 任务分类名称
   * @param relativeId 关联ID
   * @return 任务列表
   */
  public List<TaskItem> queryTasksByRelativeId(String classifier, String relativeId) {
    return queryTasksByRelativeIds(classifier, Lists.newArrayList(relativeId));
  }

  /**
   * 根据关联ID列表查询任务列表。
   *
   * @param classifier 任务分类名称
   * @param relativeIdList 关联ID列表
   * @return 任务列表
   */
  public List<TaskItem> queryTasksByRelativeIds(String classifier, List<String> relativeIdList) {
    return taskDao.queryTasksByRelativeIds(classifier, relativeIdList, taskTableName);
  }

  /**
   * 取消一个或多个异步任务.
   *
   * @param reason 取消原因
   * @param taskIds 任务ID列表
   * @return int 成功取消数量
   */
  public int cancel(String reason, List<String> taskIds) {
    taskIds.forEach(
        taskId -> {
          val taskItem = taskDao.find(taskId, taskTableName);
          zsetCommands.zrem(queueKey.apply(taskItem.getRunAt().getMillis()), taskId);
        });

    return taskDao.cancelTasks(reason, taskIds, TaskItem.待运行, TaskItem.已取消, taskTableName);
  }

  /** 刚启动时，查询所有可以执行的任务，添加到执行列表中。 */
  public void initialize() {
    initialize("default");
  }

  /**
   * 刚启动时，查询所有可以执行的任务，添加到执行列表中。
   *
   * @param classifier 任务分类名称
   */
  public void initialize(String classifier) {
    val tasks = taskDao.listReady(TaskItem.待运行, classifier, taskTableName);
    if (tasks.isEmpty()) return;

    triggerTask(tasks);
  }

  public List<TaskItem> triggerTask(List<TaskItem> tasks) {
    tasks.forEach(
        (taskItem -> {
          val taskId = createTaskIdWithVersionNumber(taskItem);
          HashMap<String, Double> map = new HashMap<>();
          map.put(taskId, (double) taskItem.getRunAt().getMillis());
          zsetCommands.zadd(queueKey.apply(taskItem.getRunAt().getMillis()), map);
        }));

    return tasks;
  }

  /**
   * 循环运行，检查是否有任务，并且运行任务。
   *
   * @param async 是否异步执行
   */
  public void run(boolean async) {
    loopStopped = false;

    while (!loopStopped) {
      fire(-1, async);
      if (Threadx.randomSleepMillis(100, 500)) break;
    }
  }

  /**
   * 触发一次任务运行。尝试获取一条任务，并执行之。
   *
   * @return 是否成功获取任务并执行。
   */
  public boolean fire() {
    return fire(1, false) > 0;
  }

  /**
   * 运行一次任务。此方法需要放在循环中调用，或者每秒触发一次，以保证实时性。
   *
   * @param max 最大数量，-1不限制
   * @param async 是否异步执行
   * @return 成功从队列中抢到任务的数量。
   */
  public int fire(int max, boolean async) {
    int shot = 0;
    Set<String> excludedTaskIds = Sets.newHashSet();

    TAG:
    while (true) {
      val taskIds =
          zsetCommands.zrangeByScore(
              queueKey.apply(System.currentTimeMillis()), 0, System.currentTimeMillis(), 0, max);
      if (taskIds.isEmpty() || excludedTaskIds.containsAll(taskIds)) break;

      for (val taskId : taskIds) {
        val p = parseVersionNumber(taskId);
        if (p.getLeft() > versionNumber) {
          excludedTaskIds.add(taskId);
          continue;
        }

        val zrem = zsetCommands.zrem(queueKey.apply(System.currentTimeMillis()), taskId);
        if (zrem < 1) {
          continue; // 该任务已经被其它人抢走了
        }

        ++shot;

        if (async && executorService != null) {
          executorService.submit(() -> fire(p.getRight()));
        } else {
          fire(p.getRight());
        }

        if (max > 0 && shot >= max) break TAG;
      }
    }

    return shot;
  }

  private String createTaskIdWithVersionNumber(TaskItem x) {
    return x.getVersionNumber() == 0
        ? x.getTaskId()
        : x.getTaskId() + VERSION_NUMBER_SEP + x.getVersionNumber();
  }

  private Pair<Long, String> parseVersionNumber(String taskId) {
    int pos = taskId.lastIndexOf(VERSION_NUMBER_SEP);
    if (pos < 0) return Pair.of(0L, taskId);

    Long vm = Longs.tryParse(taskId.substring(pos + 1));
    if (vm == null) return Pair.of(0L, taskId);

    return Pair.of(vm, taskId.substring(0, pos));
  }

  /**
   * 根据ID查找任务。
   *
   * @param taskId 任务ID
   * @return 找到的任务
   */
  public Optional<TaskItem> find(String taskId) {
    val task = taskDao.find(taskId, taskTableName);
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
    task.setStartTime(now());
    task.setState(TaskItem.运行中);
    try {
      taskDao.start();
      int changed =
          taskDao.start(task, TaskItem.待运行, now().minusMinutes(lastRunMinutesAgo), taskTableName);
      if (changed == 0) {
        log.debug("任务状态不是待运行，或者上次开始时间不在{}分钟以前， task={}", lastRunMinutesAgo, task);

        // 对于定期执行的，排定好下次的运行时间
        // 解决问题：开发环境和测试环境连接同一个数据库，但是拥有不同的redis，然后开发环境抢到了任务，导致任务不能在测试环境继续运行。
        // 开发环境抢到了任务后，若执行成功，也是在本机redis上放置下一次运行时间的触发器；若中断执行，导致任务一直处于"待运行"状态。
        // 因此此处添加下一次运行时间，保证测试环境对任务的下一次运行时间触发。
        addNextFireTime4ScheduledTask(task);
        return;
      }

      val taskable = taskableFunction.apply(task.getTaskService());
      val pair =
          TaskUtil.timeoutRun(executorService, () -> fire(taskable, task), task.getTimeout());
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
    } finally {
      taskDao.commit();
      taskDao.close();
    }
  }

  private TaskResult fire(Taskable taskable, TaskItem task) {
    taskable.beforeRun(task);
    Throwable ex = null;
    try {
      return taskable.run(task);
    } catch (Exception e) {
      ex = e;
      taskable.afterRun(task, Optional.of(e));
      throw e;
    } finally {
      if (ex == null) taskable.afterRun(task, Optional.empty());
    }
  }

  private void endTask(TaskItem task, String finalState, TaskResult result) {
    task.setState(finalState);
    task.setResultState(result.getResultState());
    task.setEndTime(now());

    addNextFireTime4ScheduledTask(task);

    if (StringUtils.isNotEmpty(task.getResultStore())) {
      resultStoreFunction.apply(task.getResultStore()).store(task, result);
    }
    taskDao.end(task, TaskItem.运行中, taskTableName);

    if (result.isFireAgain()) triggerTask(task, DateTime.now());
  }

  private void addNextFireTime4ScheduledTask(TaskItem task) {
    if (StringUtils.isEmpty(task.getScheduled())) return;

    val cron = CronAlias.create(task.getScheduled());
    val nextRunAt = cron.nextTimeAfter(task.getStartTime());
    task.setRunAt(nextRunAt);
    task.setState(TaskItem.待运行);

    triggerTask(task, nextRunAt);
  }

  private void triggerTask(TaskItem task, DateTime nextRunAt) {
    val millis = nextRunAt.getMillis();
    val map = of(createTaskIdWithVersionNumber(task), (double) millis);
    zsetCommands.zadd(queueKey.apply(millis), map);
  }
}
