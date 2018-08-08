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
 RELATIVE_ID varchar(20) NULL COMMENT '关联ID,比如订单ID\会员卡ID\排期ID等',
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
 ATTACHMENT   TEXT  NULL COMMENT '附件',
 VAR1 varchar(100) NULL COMMENT '参数1',
 VAR2 varchar(100) NULL COMMENT '参数2',
 VAR3 varchar(100) NULL COMMENT '参数3',
 CREATE_TIME datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 PRIMARY KEY (TASK_ID),
 INDEX `IDX1_T_DELAY_TASK` (`RELATIVE_ID`),
 INDEX `IDX2_T_DELAY_TASK` (`CLASSIFIER`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '任务表';
```

# Code Demo

## Java Code
```java

@Component @Slf4j
public class TaskServiceScheduling {
    @Autowired TaskRunner taskRunner;

    @Scheduled(fixedDelay = 1000, initialDelay = 10000)
    public void tasking() {
        taskRunner.fire(); // fire every 1 second
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