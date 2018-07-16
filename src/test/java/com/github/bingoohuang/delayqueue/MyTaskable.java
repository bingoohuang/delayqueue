package com.github.bingoohuang.delayqueue;

import org.springframework.stereotype.Service;

@Service
public class MyTaskable implements Taskable {

    @Override public String run(TaskItem taskItem) {
        return "OK";
    }
}
