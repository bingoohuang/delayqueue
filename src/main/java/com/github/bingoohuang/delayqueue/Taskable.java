package com.github.bingoohuang.delayqueue;

public interface Taskable {
    /**
     * 运行任务。
     *
     * @param taskItem 任务细节。
     * @return 运行结果
     */
    String run(TaskItem taskItem);
}
