package com.github.bingoohuang.delayqueue.spring;

import com.github.bingoohuang.delayqueue.Taskable;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringTaskableFactory implements ApplicationContextAware {
    private ApplicationContext appContext;

    public Taskable getTaskService(String taskService) {
        return (Taskable) appContext.getBean(StringUtils.uncapitalize(taskService));
    }

    @Override public void setApplicationContext(ApplicationContext appContext) {
        this.appContext = appContext;
    }
}
