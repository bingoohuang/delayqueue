package com.github.bingoohuang.delayqueue;

import lombok.val;
import org.joda.time.DateTime;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class CronAliasTest {
    @Test
    public void at1() {
        val cron = CronAlias.create("@at 12:00");
        val dt = DateTime.parse("2018-10-30T20:07:23");
        assertThat(cron.nextTimeAfter(dt)).isEqualTo(DateTime.parse("2018-10-31T12:00:00"));
    }

    @Test
    public void at2() {
        val cron = CronAlias.create("@at ??:30");
        val dt = DateTime.parse("2018-10-30T20:07:23");
        assertThat(cron.nextTimeAfter(dt)).isEqualTo(DateTime.parse("2018-10-30T20:30:00"));
    }

    @Test
    public void every15m() {
        val cron = CronAlias.create("@every 15m");
        val dt = DateTime.parse("2018-10-30T20:07:23");
        assertThat(cron.nextTimeAfter(dt)).isEqualTo(DateTime.parse("2018-10-30T20:15:00"));
    }

    @Test
    public void every2h() {
        val cron = CronAlias.create("@every 2h");
        val dt = DateTime.parse("2018-10-30T20:07:23");
        assertThat(cron.nextTimeAfter(dt)).isEqualTo(DateTime.parse("2018-10-30T22:00:00"));
    }
}