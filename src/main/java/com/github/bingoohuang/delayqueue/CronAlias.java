package com.github.bingoohuang.delayqueue;

import com.google.common.collect.ImmutableMap;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;

import java.util.Map;
import java.util.regex.Pattern;

public class CronAlias {
    private static final String
            MONTHLY = "0 0 1 * *",
            WEEKLY = "0 0 * * 0",
            DAILY = "0 0 * * *",
            HOURLY = "0 * * * *";

    private static final Map<String, String> ALIASES = new ImmutableMap.Builder<String, String>()
            .put("monthly", MONTHLY)
            .put("weekly", WEEKLY)
            .put("daily", DAILY)
            .put("hourly", HOURLY)
            .build();

    public static CronExpression create(String expr) {
        return new CronExpression(alias(expr), false);
    }

    private static String alias(String expr) {
        if (expr.charAt(0) != '@') return expr;

        val alias = expr.substring(1);
        val aliasExpr = ALIASES.get(alias);
        if (aliasExpr != null) return aliasExpr;

        if (StringUtils.startsWithIgnoreCase(alias, "Every")) {
            return parseEveryExpr(alias.substring("Every".length()));
        } else if (StringUtils.startsWithIgnoreCase(alias, "At")) {
            return parseAtExpr(alias.substring("At".length()));
        }

        throw new IllegalArgumentException("unknown alias " + expr); //$NON-NLS-1$
    }

    private static Pattern atExprPattern = Pattern.compile(
            "\\s+(\\d\\d|\\?\\?):(\\d\\d)", Pattern.CASE_INSENSITIVE);

    private static String parseAtExpr(String atExpr) {
        val matcher = atExprPattern.matcher(atExpr);
        if (!matcher.matches()) throw new RuntimeException(atExpr + " is not valid");

        if (matcher.group(1).equals("??")) { // eg. at ??:40
            return matcher.group(2) + " * * * ?";
        }

        val formatter = DateTimeFormat.forPattern("HH:mm");
        val dateTime = formatter.parseDateTime(matcher.group().trim());

        return dateTime.getMinuteOfHour() + " " + dateTime.getHourOfDay() + " * * *";
    }

    private static Pattern everyExprPattern = Pattern.compile(
            "\\s+(\\d+)\\s*(h|hour|m|minute|s|second)s?", Pattern.CASE_INSENSITIVE);

    private static String parseEveryExpr(String everyExpr) {
        val matcher = everyExprPattern.matcher(everyExpr);
        if (!matcher.matches()) throw new RuntimeException(everyExpr + " is not valid");

        int num = Integer.parseInt(matcher.group(1));
        if (num <= 0) throw new RuntimeException(everyExpr + " is not valid");
        char unit = matcher.group(2).charAt(0);
        switch (unit) {
            case 'h':
            case 'H':
                return "0 */" + num + " * * *";
            case 'm':
            case 'M':
                return "*/" + num + " * * * *";
        }

        throw new RuntimeException(everyExpr + " is not valid");
    }

}
