package com.github.bingoohuang.delayqueue;

import java.util.Optional;

public interface Taskable {
    /**
     * 运行任务之前做点事（可以做一些前置处理，比如设置一些环境变量等）。
     *
     * @param taskItem 任务。
     */
    default void beforeRun(TaskItem taskItem) {

    }


    /**
     * 运行任务。
     *
     * @param taskItem 任务细节。
     * @return 运行结果
     */
    String run(TaskItem taskItem);

    /**
     * 运行任务之后做点事（可以做一些前置处理，比如清除一些环境变量等）。
     *
     * @param taskItem 任务。
     * @param ex       异常
     */
    default void afterRun(TaskItem taskItem, Optional<Throwable> ex) {

    }

}
