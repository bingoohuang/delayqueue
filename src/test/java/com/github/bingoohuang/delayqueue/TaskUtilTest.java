package com.github.bingoohuang.delayqueue;

import org.joda.time.DateTime;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class TaskUtilTest {

    @Test
    public void randomSleep() {
        Thread.currentThread().interrupt();
        TaskUtil.randomSleepMillis(10, 20);
    }

    @Test
    public void emptyThenNow() {
        assertThat(TaskUtil.emptyThenNow(null)).isNotNull();
        assertThat(TaskUtil.emptyThenNow(DateTime.now())).isNotNull();
    }

    public interface SubInterface {

    }

    public static class Sub {

    }

    @Test
    public void adapt() {
        TaskUtil taskUtil = new TaskUtil();
        assertThat(TaskUtil.adapt(taskUtil, TaskUtil.class)).isSameAs(taskUtil);

        assertThat(TaskUtil.adapt(taskUtil, SubInterface.class)).isInstanceOf(SubInterface.class);
        assertThat(TaskUtil.adapt(taskUtil, Sub.class)).isInstanceOf(Sub.class);
    }
}