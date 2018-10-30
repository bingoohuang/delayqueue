package com.github.bingoohuang.delayqueue.spring;

import com.github.bingoohuang.delayqueue.TaskItem;
import org.n3r.eql.eqler.annotations.Dynamic;
import org.n3r.eql.eqler.annotations.Eqler;
import org.n3r.eql.eqler.annotations.Sql;

import java.util.List;

@Eqler
public interface TaskDao {
    @Sql("insert into $$(TASK_ID, RELATIVE_ID, CLASSIFIER, TASK_NAME, TASK_SERVICE, STATE, RUN_AT, " +
            "TIMEOUT, START_TIME, END_TIME, RESULT, ATTACHMENT, VAR1,VAR2,VAR3, " +
            "SCHEDULED, RESULT_STORE, CREATE_TIME, VERSION_NUMBER) " +
            "values  /* for item=t collection=_1 separator=, */ " +
            "(#t.taskId#, #t.relativeId#, #t.classifier#, #t.taskName#, #t.taskService#, #t.state#, #t.runAt#, " +
            " #t.timeout#, #t.startTime#, #t.endTime#, #t.result#, #t.attachment#, #t.var1#, #t.var2#, #t.var3#, " +
            "#t.scheduled#, #t.resultStore#, #t.createTime#, #t.versionNumber#) " +
            " /* end */ ")
    int add(List<TaskItem> tasks, @Dynamic String taskTableName);

    String SELECT_CLAUSE = "select TASK_ID, RELATIVE_ID, CLASSIFIER, TASK_NAME, TASK_SERVICE, STATE, RUN_AT, " +
            "TIMEOUT, START_TIME, END_TIME, RESULT_STATE, RESULT_STORE, RESULT, ATTACHMENT, VAR1,VAR2,VAR3, SCHEDULED, CREATE_TIME, VERSION_NUMBER " +
            "from $$";

    @Sql(SELECT_CLAUSE + " where TASK_ID = ## ")
    TaskItem find(String taskId, @Dynamic String taskTableName);

    @Sql(SELECT_CLAUSE + " where STATE = ## and RUN_AT < now() and CLASSIFIER = ##")
    List<TaskItem> listReady(String state, String classifier, @Dynamic String taskTableName);

    @Sql(SELECT_CLAUSE + " where CLASSIFIER = #_1# and RELATIVE_ID in (/* in _2 */) ")
    List<TaskItem> queryTaskIdsByRelativeIds(String classifier, List<String> relativeIds, @Dynamic String taskTableName);

    @Sql("update $$ set STATE = #_1.state#, START_TIME = #_1.startTime# where TASK_ID = #_1.taskId# and STATE = #_2#")
    int start(TaskItem task, String fromState, @Dynamic String taskTableName);

    @Sql("update $$ set RELATIVE_ID = #_1.relativeId#, STATE = #_1.state#, RUN_AT = #_1.runAt#, ATTACHMENT = #_1.attachment#, " +
            "VAR1 = #_1.var1#, VAR2 = #_1.var2#, VAR3 = #_1.var3#, " +
            "END_TIME = #_1.endTime#, " +
            "RESULT_STATE = #_1.resultState#, RESULT = #_1.result# " +
            "where TASK_ID = #_1.taskId# and STATE = #_2#")
    int end(TaskItem task, String fromState, @Dynamic String taskTableName);

    @Sql("update $$ set STATE = #_4#, END_TIME = NOW(), RESULT_STATE = #_1# where TASK_ID in (/* in _2 */) and STATE = #_3#")
    int cancelTasks(String reason, List<String> taskIds, String fromState, String toState, @Dynamic String taskTableName);
}
