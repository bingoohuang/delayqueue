package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.westcache.utils.FastJsons;

public class DirectResultStore implements ResultStoreable {
  @Override
  public void store(TaskItem taskItem, TaskResult taskResult) {
    taskItem.setResultState(taskResult.getResultState());
    taskItem.setResult(FastJsons.json(taskResult.getResult()));
  }

  @Override
  public void load(TaskItem taskItem) {}
}
