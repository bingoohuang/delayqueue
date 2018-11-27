package com.github.bingoohuang.delayqueue;

import org.springframework.stereotype.Component;

@Component
public class FireAgainTaskable implements Taskable {
    @Override public TaskResult run(TaskItem taskItem) {
        return TaskResult.fireAgain("OK");
    }
}
