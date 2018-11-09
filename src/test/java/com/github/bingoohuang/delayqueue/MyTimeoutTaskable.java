package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.utils.lang.Threadx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service @Slf4j
public class MyTimeoutTaskable implements Taskable {

    @Override public TaskResult run(TaskItem taskItem) {
        log.debug("超时任务开始");
        Threadx.randomSleepMillis(1100, 1200);
        log.debug("超时任务结束");
        return TaskResult.OK;
    }
}
