package com.github.bingoohuang.delayqueue;

import org.springframework.stereotype.Service;

@Service
public class MyTaskable implements Taskable {
    @Override public TaskResult run(TaskItem taskItem) {
        return TaskResult.of("OK");
    }

    @Service
    public static class Inner1Task implements Taskable {
        @Override public TaskResult run(TaskItem taskItem) {
            return TaskResult.of("OK");
        }

        @Service
        public static class Inner2Task implements Taskable {
            @Override public TaskResult run(TaskItem taskItem) {
                return TaskResult.of("OK");
            }
        }
    }
}
