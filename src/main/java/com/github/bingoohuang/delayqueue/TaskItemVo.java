package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.westcache.utils.FastJsons;
import com.github.bingoohuang.westid.WestId;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

@Value @Builder
public class TaskItemVo {
    private final String taskId;          // 可选：任务ID
    private final String relativeId;      // 可选：关联ID，例如订单ID，卡ID，课程ID，审批流程ID等。
    private final String classifier;      // 可选：任务分类名称，例如子系统名称。
    private final String taskName;        // 必须：任务名称
    private final String taskService;     // 必须：任务执行服务名称
    private final DateTime runAt;         // 可选：可以开始运行的时间，设定在将来，获得延时执行
    private final int timeout;            // 可选：任务超时秒数
    private final Object attachment;      // 可选：任务附件（必须可JSON化）
    private final String var1, var2, var3;  // 可选：参数

    public TaskItem createTaskItem() {
        return TaskItem.builder()
                .taskId(StringUtils.defaultString(getTaskId(), String.valueOf(WestId.next())))
                .relativeId(getRelativeId())
                .classifier(getClassifier())
                .taskName(checkNotEmpty(getTaskName(), "任务名称不可缺少"))
                .taskService(checkNotEmpty(getTaskService(), "任务执行服务名称不可缺少"))
                .state(TaskItem.待运行)
                .runAt(getRunAt())
                .timeout(getTimeout())
                .attachment(FastJsons.json(getAttachment()))
                .var1(getVar1())
                .var2(getVar2())
                .var3(getVar3())
                .createTime(DateTime.now())
                .build();
    }

    private String checkNotEmpty(String str, String desc) {
        if (isNotEmpty(str)) return str;

        throw new RuntimeException(desc);
    }

    public DateTime getRunAt() {
        return Util.emptyThenNow(runAt);
    }


    public String getClassifier() {
        return classifier != null ? classifier : "default";
    }


}
