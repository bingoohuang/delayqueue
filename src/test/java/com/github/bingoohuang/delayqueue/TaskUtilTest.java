package com.github.bingoohuang.delayqueue;

import org.joda.time.DateTime;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class TaskUtilTest {

    @Test
    public void randomSleep() {
        new TaskUtil();
        Thread.currentThread().interrupt();
        TaskUtil.randomSleepMillis(10, 20);
    }

    @Test
    public void emptyThenNow() {
        assertThat(TaskUtil.emptyThenNow(null)).isNotNull();
        assertThat(TaskUtil.emptyThenNow(DateTime.now())).isNotNull();
    }
}