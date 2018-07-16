package com.github.bingoohuang.delayqueue;

import org.n3r.eql.eqler.annotations.Dynamic;
import org.n3r.eql.eqler.annotations.Sql;

import java.util.List;

/*

-- for mysql

DROP TABLE IF EXISTS t_delay_task;
CREATE TABLE t_delay_task (
 TASK_ID varchar(20) NOT NULL COMMENT '任务ID',
 RELATIVE_ID varchar(20) NULL COMMENT '关联ID',
 TASK_NAME varchar(100) NOT NULL COMMENT '任务名称',
 TASK_SERVICE varchar(100) NOT NULL COMMENT '任务服务名称，需要实现Taskable接口',
 STATE varchar(3) NOT NULL COMMENT '待运行/运行中/已成功/已失败/已取消/已超时',
 READY_TIME datetime NOT NULL COMMENT '可以开始运行的时间，此参数可以设置延时',
 TIMEOUT tinyint NOT NULL DEFAULT 0 COMMENT '超时时间（秒）',
 START_TIME datetime NULL COMMENT '开始运行时间',
 END_TIME datetime NULL COMMENT '结束运行时间',
 RESULT varchar(300) NULL COMMENT '任务运行结果',
 ATTACHMENT   TEXT  NULL COMMENT '附件',
 UPDATE_TIME datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
 CREATE_TIME datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 PRIMARY KEY (TASK_ID)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '任务表';
 */
public interface TaskDao {
    @Sql("insert into $$(TASK_ID, RELATIVE_ID, TASK_NAME, TASK_SERVICE, STATE, READY_TIME, " +
            "TIMEOUT, START_TIME, END_TIME, RESULT, ATTACHMENT, UPDATE_TIME, CREATE_TIME)" +
            "values(#?#, #?#, #?#, #?#, #?#, #?#, #?#, #?#, #?#, #?#, #?#, #?#, #?#)")
    int add(TaskItem task, @Dynamic String taskTableName);

    @Sql("insert into $$(TASK_ID, RELATIVE_ID, TASK_NAME, TASK_SERVICE, STATE, READY_TIME, " +
            "TIMEOUT, START_TIME, END_TIME, RESULT, ATTACHMENT, UPDATE_TIME, CREATE_TIME) " +
            "values " +
            " /* for item=t collection=_1 separator=, */" +
            "(#t.taskId#, #t.RelativeId#, #t.TaskName#, #t.TaskService#, #t.State#, #t.ReadyTime#, #t.Timeout#, #t.StartTime#, #t.EndTime#, #t.Result#, #t.Attachment#, #t.UpdateTime#, #t.CreateTime#) " +
            " /* end */ ")
    int add(List<TaskItem> tasks, @Dynamic String taskTableName);

    @Sql("select TASK_ID, RELATIVE_ID, TASK_NAME, TASK_SERVICE, STATE, READY_TIME, TIMEOUT, START_TIME, END_TIME, RESULT, ATTACHMENT, UPDATE_TIME, CREATE_TIME " +
            "from $$ " +
            "where TASK_ID = ## ")
    TaskItem find(String taskId, @Dynamic String taskTableName);

    @Sql("select TASK_ID, RELATIVE_ID, TASK_NAME, TASK_SERVICE, STATE, READY_TIME, TIMEOUT, START_TIME, END_TIME, RESULT, ATTACHMENT, UPDATE_TIME, CREATE_TIME " +
            "from $$ " +
            "where STATE = '待运行' and READY_TIME < now() ")
    List<TaskItem> listReady(@Dynamic String taskTableName);


    @Sql("update $$ set STATE = '运行中', START_TIME = #?# where TASK_ID = #?# and STATE = '待运行'")
    int start(TaskItem task, @Dynamic String taskTableName);

    @Sql("update $$ set STATE = #?#, END_TIME = #?#, RESULT = #?# where TASK_ID = #?# and STATE = '运行中'")
    int end(TaskItem task, @Dynamic String taskTableName);


    @Sql("update $$ set STATE = '已取消', END_TIME = NOW(), RESULT = #_1# where TASK_ID in (/* in _2 */) and STATE = '待运行'")
    int cancelTasks(@Dynamic String taskTableName, String reason, String... taskIds);
}
