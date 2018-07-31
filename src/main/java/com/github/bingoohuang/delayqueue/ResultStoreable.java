package com.github.bingoohuang.delayqueue;

public interface ResultStoreable {
    void store(TaskItem taskItem, TaskResult taskResult);
    void load(TaskItem taskItem);
}
