package com.github.bingoohuang.delayqueue;

import lombok.Value;

@Value
public class TaskResult {
    public static final TaskResult OK = of("OK");
    private final String resultState;
    private final Object result;

    public static TaskResult of(String message) {
        return new TaskResult(message, null);
    }

    public static TaskResult of(String message, Object result) {
        return new TaskResult(message, result);
    }
}
