package com.github.bingoohuang.delayqueue;

public interface TaskableFactory {
    Taskable getTaskable(String taskService);
}
