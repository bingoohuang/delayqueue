package com.github.bingoohuang.delayqueue;

import org.joda.time.DateTime;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

public class TaskUtilTest {

    @Test
    public void randomSleep() {
        Thread.currentThread().interrupt();
        TaskUtil.randomSleepMillis(10, 20);
    }

    @Test
    public void emptyThenNow() {
        assertThat(TaskUtil.emptyThenNow(null, null)).isNotNull();
        assertThat(TaskUtil.emptyThenNow(DateTime.now(), null)).isNotNull();
    }

    public interface SubInterface {
        void doSome();
    }

    public static class Sub {

    }

    @Test
    public void adapt() {
        TaskUtil taskUtil = new TaskUtil();
        assertThat(TaskUtil.adapt(taskUtil, TaskUtil.class)).isSameAs(taskUtil);

        SubInterface adapt = TaskUtil.adapt(taskUtil, SubInterface.class);
        assertThat(adapt).isInstanceOf(SubInterface.class);
        assertThat(TaskUtil.adapt(taskUtil, Sub.class)).isInstanceOf(Sub.class);

        try {
            adapt.doSome();
            fail();
        } catch (Exception ex) {
            assertThat(ex.getCause()).isInstanceOf(NoSuchMethodException.class);
        }
    }
}