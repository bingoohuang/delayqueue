package com.github.bingoohuang.delayqueue.spring;

import com.github.bingoohuang.delayqueue.DirectResultStore;
import org.n3r.eql.eqler.spring.EqlerScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@EqlerScan
@Configuration
@ComponentScan
public class TaskSpringConfig {
  @Bean
  public DirectResultStore directResultStore() {
    return new DirectResultStore();
  }
}
