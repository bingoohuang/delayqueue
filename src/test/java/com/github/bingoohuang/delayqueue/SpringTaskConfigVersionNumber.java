package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.delayqueue.spring.SpringTaskConfig;
import org.springframework.stereotype.Component;

@Component
public class SpringTaskConfigVersionNumber extends SpringTaskConfig {
    @Override public long getVersionNumber() {
        return 20181016L;
    }
}
