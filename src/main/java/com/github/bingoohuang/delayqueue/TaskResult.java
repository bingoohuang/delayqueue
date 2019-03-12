package com.github.bingoohuang.delayqueue;

import lombok.Value;

@Value
public class TaskResult {
  public static final TaskResult OK = of("OK");

  private final String resultState;
  private final Object result;
  private final boolean fireAgain;

  public static TaskResult of(String message) {
    return new TaskResult(message, null, false);
  }

  /**
   * 再次触发任务执行。
   *
   * @param message 直接结果消息
   * @return 任务结果
   */
  public static TaskResult fireAgain(String message) {
    return new TaskResult(message, null, true);
  }

  public static TaskResult of(String message, Object result) {
    return new TaskResult(message, result, false);
  }
}
