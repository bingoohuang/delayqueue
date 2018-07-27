package com.github.bingoohuang.delayqueue.spring;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(TaskSpringConfig.class)
public @interface TaskSpringEnabled {

}
