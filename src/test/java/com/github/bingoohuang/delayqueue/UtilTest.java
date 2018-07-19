package com.github.bingoohuang.delayqueue;

import org.joda.time.DateTime;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;

public class UtilTest {

    @Test
    public void randomSleep() {
        new Util();
        Thread.currentThread().interrupt();
        Util.randomSleep(10, 20, TimeUnit.MILLISECONDS);
    }

    @Test
    public void emptyThenNow() {
        assertThat(Util.emptyThenNow(null)).isNotNull();
        assertThat(Util.emptyThenNow(DateTime.now())).isNotNull();
    }
}