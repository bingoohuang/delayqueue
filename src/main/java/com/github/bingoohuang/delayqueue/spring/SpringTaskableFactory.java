package com.github.bingoohuang.delayqueue.spring;

import com.github.bingoohuang.delayqueue.Taskable;
import com.github.bingoohuang.delayqueue.TaskableFactory;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class SpringTaskableFactory implements TaskableFactory, ApplicationContextAware {
    private ApplicationContext appContext;

    @Override public Taskable getTaskable(String taskService) {
        return (Taskable) appContext.getBean(StringUtils.uncapitalize(taskService));
    }

    @Override public void setApplicationContext(ApplicationContext appContext) {
        this.appContext = appContext;
    }
}
