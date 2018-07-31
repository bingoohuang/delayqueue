package com.github.bingoohuang.delayqueue;

import org.springframework.stereotype.Component;

@Component
public class MyInvokeTaskable implements Taskable {
    @Override public TaskResult run(TaskItem taskItem) {
        return TaskResult.of("成功", "DANGDANGDANG");
    }
}
