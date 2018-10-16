package com.github.bingoohuang.delayqueue.spring;

import com.github.bingoohuang.delayqueue.TaskRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class SpringTaskRunner extends TaskRunner {
    public SpringTaskRunner(@Autowired @Qualifier("springTaskConfig") SpringTaskConfig config) {
        super(config);
    }
}
