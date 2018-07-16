package com.github.bingoohuang.delayqueue;

import org.springframework.stereotype.Service;

@Service
public class MyTaskable implements Taskable {

    @Override public void run(TaskItem taskItem) {
        System.out.println(taskItem);
    }
}
