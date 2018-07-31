package com.github.bingoohuang.delayqueue;

import org.springframework.stereotype.Service;

@Service
public class MyTaskable implements Taskable {

    @Override public TaskResult run(TaskItem taskItem) {
        return TaskResult.of("OK");
    }
}
