package com.github.bingoohuang.delayqueue.spring;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringBeanFactory implements ApplicationContextAware {
    private ApplicationContext appContext;

    @SuppressWarnings("unchecked")
    public <T> T getBean(String name) {
        return (T) appContext.getBean(StringUtils.uncapitalize(name));
    }

    @Override public void setApplicationContext(ApplicationContext appContext) {
        this.appContext = appContext;
    }
}
