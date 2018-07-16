package com.github.bingoohuang.delayqueue;

import org.springframework.stereotype.Service;

@Service
public class MyExTaskable implements Taskable {

    @Override public String run(TaskItem taskItem) {
        throw new RuntimeException("😡，竟然崩溃了，泪奔");
    }
}
