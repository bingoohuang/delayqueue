package com.github.bingoohuang.delayqueue;

import org.n3r.eql.eqler.annotations.Dynamic;
import org.n3r.eql.eqler.annotations.Sql;

import java.util.List;

public interface TaskDao {
    @Sql("insert into $$(TASK_ID, RELATIVE_ID, TASK_NAME, TASK_SERVICE, STATE, RUN_AT, " +
            "TIMEOUT, START_TIME, END_TIME, RESULT, ATTACHMENT, CREATE_TIME)" +
            "values(#?#, #?#, #?#, #?#, #?#, #?#, #?#, #?#, #?#, #?#, #?#, #?#)")
    int add(TaskItem task, @Dynamic String taskTableName);

    @Sql("insert into $$(TASK_ID, RELATIVE_ID, TASK_NAME, TASK_SERVICE, STATE, RUN_AT, " +
            "TIMEOUT, START_TIME, END_TIME, RESULT, ATTACHMENT, CREATE_TIME) " +
            "values " +
            " /* for item=t collection=_1 separator=, */" +
            "(#t.taskId#, #t.relativeId#, #t.taskName#, #t.taskService#, #t.state#, #t.runAt#, #t.timeout#, #t.startTime#, #t.endTime#, #t.result#, #t.attachment#, #t.createTime#) " +
            " /* end */ ")
    int add(List<TaskItem> tasks, @Dynamic String taskTableName);

    @Sql("select TASK_ID, RELATIVE_ID, TASK_NAME, TASK_SERVICE, STATE, RUN_AT, TIMEOUT, START_TIME, END_TIME, RESULT, ATTACHMENT, CREATE_TIME " +
            "from $$ " +
            "where TASK_ID = ## ")
    TaskItem find(String taskId, @Dynamic String taskTableName);

    @Sql("select TASK_ID, RELATIVE_ID, TASK_NAME, TASK_SERVICE, STATE, RUN_AT, TIMEOUT, START_TIME, END_TIME, RESULT, ATTACHMENT, CREATE_TIME " +
            "from $$ " +
            "where STATE = '待运行' and RUN_AT < now() ")
    List<TaskItem> listReady(@Dynamic String taskTableName);


    @Sql("update $$ set STATE = '运行中', START_TIME = #?# where TASK_ID = #?# and STATE = '待运行'")
    int start(TaskItem task, @Dynamic String taskTableName);

    @Sql("update $$ set STATE = #?#, END_TIME = #?#, RESULT = #?# where TASK_ID = #?# and STATE = '运行中'")
    int end(TaskItem task, @Dynamic String taskTableName);


    @Sql("update $$ set STATE = '已取消', END_TIME = NOW(), RESULT = #_1# where TASK_ID in (/* in _2 */) and STATE = '待运行'")
    int cancelTasks(String reason, List<String> taskIds, @Dynamic String taskTableName);

    @Sql("select TASK_ID, RELATIVE_ID, TASK_NAME, TASK_SERVICE, STATE, RUN_AT, TIMEOUT, START_TIME, END_TIME, RESULT, ATTACHMENT, CREATE_TIME " +
            "from $$ where RELATIVE_ID in (/* in _1 */)")
    List<TaskItem> queryTaskIdsByRelativeIds(List<String> relativeIds, @Dynamic String taskTableName);
}
