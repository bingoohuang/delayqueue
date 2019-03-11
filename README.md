# delayqueue

[![Build Status](https://travis-ci.org/bingoohuang/delayqueue.svg?branch=master)](https://travis-ci.org/bingoohuang/delayqueue)
[![Coverage Status](https://coveralls.io/repos/github/bingoohuang/delayqueue/badge.svg?branch=master)](https://coveralls.io/github/bingoohuang/delayqueue?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.bingoohuang/delayqueue/badge.svg?style=flat-square)](https://maven-badges.herokuapp.com/maven-central/com.github.bingoohuang/delayqueue/)
[![License](http://img.shields.io/:license-apache-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

delay queue based on redis. 

# Task Table DDL

```sql
-- MySQL
DROP TABLE IF EXISTS t_delay_task;
CREATE TABLE t_delay_task (
 TASK_ID varchar(20) NOT NULL COMMENT '任务ID',
 RELATIVE_ID varchar(100) NULL COMMENT '关联ID,比如订单ID\会员卡ID\排期ID等',
 CLASSIFIER varchar(20) NOT NULL COMMENT '任务分类',
 TASK_NAME varchar(100) NOT NULL COMMENT '任务名称',
 TASK_SERVICE varchar(100) NOT NULL COMMENT '任务服务名称，需要实现Taskable接口',
 STATE varchar(3) NOT NULL COMMENT '待运行/运行中/已成功/已失败/已取消/已超时',
 RUN_AT datetime NOT NULL COMMENT '何时运行，此参数可以设置延时',
 TIMEOUT tinyint NOT NULL DEFAULT 0 COMMENT '超时时间（秒）',
 START_TIME datetime NULL COMMENT '开始运行时间',
 END_TIME datetime NULL COMMENT '结束运行时间',
 RESULT_STATE varchar(300) NULL COMMENT '任务执行状态',
 RESULT_STORE varchar(100) NOT NULL COMMENT '结果存储方式',
 RESULT TEXT  NULL COMMENT '任务执行详细结果',
 HOSTNAME varchar(100)   NULL COMMENT '客户端机器名',
 CLIENT_IP varchar(100)  NULL COMMENT '客户端IP',
 ATTACHMENT varchar(100)  NULL COMMENT '附件',
 VAR1 varchar(100) NULL COMMENT '参数1',
 VAR2 varchar(100) NULL COMMENT '参数2',
 VAR3 varchar(100) NULL COMMENT '参数3',
 SCHEDULED varchar(50) NULL COMMENT '排期',
 CREATE_TIME datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 VERSION_NUMBER BIGINT(20) NOT NULL DEFAULT 0 COMMENT '任务创建时的程序版本号',
 PRIMARY KEY (TASK_ID),
 INDEX `IDX1_T_DELAY_TASK` (`RELATIVE_ID`),
 INDEX `IDX2_T_DELAY_TASK` (`CLASSIFIER`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '任务表';
```

# Scheduled Examples
1. cron expression, like [*/15 * * * *](https://crontab.guru/#*/15_*_*_*_*). **Note: the seconds field is not included.**
    <pre>
    +--------------------------+-----------------------------------------------+--------------------+
    | Field                    | Allowable values                              | Special Characters |
    +--------------------------+-----------------------------------------------+--------------------+
    | Minutes                  | 0-59                                          | , - * /            |
    +--------------------------+-----------------------------------------------+--------------------+
    | Hours                    | 0-23                                          | , - * /            |
    +--------------------------+-----------------------------------------------+--------------------+
    | Day of month             | 1-31                                          | , - * ? / L W      |
    +--------------------------+-----------------------------------------------+--------------------+
    | Month                    | 1-12 or JAN-DEC (note: english abbreviations) | , - * /            |
    +--------------------------+-----------------------------------------------+--------------------+
    | Day of week              | 1-7 or MON-SUN (note: english abbreviations)  | , - * ? / L #      |
    +--------------------------+-----------------------------------------------+--------------------+
    </pre>
2. @every x minutes/hours (@every x m/h)
3. @at 12:00
4. @at ??:40 (every 40 minutes of each hour)
5. @monthly @weekly @daily @hourly

# Code Demo

## Java Code
```java

@Component @Slf4j
public class TaskServiceScheduling implements ApplicationListener<ContextRefreshedEvent> {
    @Autowired TaskRunner taskRunner;
    @Override public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
       init();
    }

    @Scheduled(fixedDelay = 1000, initialDelay = 10000)
    public void tasking() {
        taskRunner.fire(); // fire every 1 second
    }
    
    
    private void init() {
        initTask("1541421482177", "处理当天没完成的测评", UnfinishedEvaluationDealTask.class, "@at 23:50");

        try {
            taskRunner.initialize();
        } catch (Exception ex) {
            log.warn("init tasks error", ex);
        }
    }
    /**
     * 主动触发任务。
     */
    public void firePaperInstanceResultTask() {
        taskRunner.fire("1540880531777");
    }
    
    private void initTask(String taskId, String taskName, Class<?> taskServiceClass, String scheduled) {
        val taskItem = taskRunner.find(taskId);
        if (taskItem.isPresent()) return;
    
        taskRunner.submit(TaskItemVo.builder()
                .taskId(taskId)
                .taskServiceClass(taskServiceClass)
                .scheduled(scheduled)
                .taskName(taskName)
                .build());
    }
}

@Configuration
@EnableScheduling  // enable @Scheduled get working
@TaskSpringEnabled // enable delayqueue spring out of box supporting
public class SpringConfig {
    
}

@Service // Define Customized Task Processing
public class MyTaskable implements Taskable {

    @Override public String run(TaskItem taskItem) {
        return "OK";
    }
}


@Service // Demo to submit a new delay task
public class SomeService {
    @Autowired TaskRunner taskRunner;
    
    public void doSomething() {
        TaskItemVo vo = TaskItemVo.builder()
                .taskName("My Task")
                .taskService(MyTaskable.class.getSimpleName())
                .attachment(attachment)
                .build();
        taskRunner.submit(vo);
       
       // demo to invoke the task (wait the task to be executed)
        TaskItem item1 = taskRunner.invoke(vo1, 3000);
        assertThat(item1.getResultAsString()).isEqualTo("DANGDANGDANG");
    }
}

```

## Customized configuration file.

a delayqueue.properties file can be put on the classpath to customized redis key and task table name, 
like in resources directory under maven project structure.
```properties
# Redis Key for the queue
QueueKey=delayqueue

# Task table name
TaskTableName=t_delay_task

```



# Redis Sorted Set

[Redis Sorted Sets](https://redis.io/topics/data-types) are, similarly to Redis Sets, non repeating collections of Strings. The difference is that every member of a Sorted Set is associated with score, that is used in order to take the sorted set ordered, from the smallest to the greatest score. While members are unique, scores may be repeated.

We put the task id to the sorted set with its runAt millis as score, [like](https://redis.io/commands/zadd) `ZADD key {runAtInMillis} {taskId} `, then the fire method will check 
the sorted set by score range, [like](https://redis.io/commands/zrangebyscore) `ZRANGEBYSCORE key 0 {currentMillis} 0 1` to try poll the first executable taskId.

# 原理说明

![image](https://user-images.githubusercontent.com/1940588/46710386-27c37f00-cc7a-11e8-9605-ce8c93cfbbd3.png)

异步任务，包括定时任务，都可以通过DelayQueue库来完成。该库通过Spring中的排程任务，轮询检查Redis中的zset中Score小于等于当前毫秒的元素（值为taskId）,然后通过taskId去数据库中查询任务的详情情况，找到执行任务的Service，然后调用该Service完成任务的调用。
在Redis的zset上轮询，效率非常高，每秒钟可以达到十万级别，而数据库的轮询效率则只有一万不到。DelayQueue很好地结合了Redis的高性能和数据库的有效存储，来完成异步任务的执行。

任务状态流图：
![image](https://user-images.githubusercontent.com/1940588/46988045-5d101700-d129-11e8-9a2e-bcfe5744cb9d.png)

