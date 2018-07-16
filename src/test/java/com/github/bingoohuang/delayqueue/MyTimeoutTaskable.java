package com.github.bingoohuang.delayqueue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service @Slf4j
public class MyTimeoutTaskable implements Taskable {

    @Override public void run(TaskItem taskItem) {
        log.debug("超时任务开始");
        Util.randomSleep(1100, 1200, TimeUnit.MILLISECONDS);
        log.debug("超时任务结束");
    }
}
