package com.github.bingoohuang.delayqueue.spring;

import com.github.bingoohuang.delayqueue.TaskRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SpringTaskRunner extends TaskRunner {
    @Autowired
    public SpringTaskRunner(SpringTaskConfig config) {
        super(config);
    }
}
