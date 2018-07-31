package com.github.bingoohuang.delayqueue;

import org.joda.time.DateTime;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;

public class DelayQueueUtilTest {

    @Test
    public void randomSleep() {
        new DelayQueueUtil();
        Thread.currentThread().interrupt();
        DelayQueueUtil.randomSleep(10, 20, TimeUnit.MILLISECONDS);
    }

    @Test
    public void emptyThenNow() {
        assertThat(DelayQueueUtil.emptyThenNow(null)).isNotNull();
        assertThat(DelayQueueUtil.emptyThenNow(DateTime.now())).isNotNull();
    }
}