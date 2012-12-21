package com.borqs.server.wutong.notif;

import com.borqs.server.base.context.Context;

import java.util.List;

public abstract class GroupNotificationSender extends NotificationSender {
    public abstract List<Long> getScope(Context ctx, String senderId, Object... args);

    public GroupNotificationSender() {
        super();
    }

    @Override
    public String getTitle(Context ctx, Object... args)
    {
        return "";
    }

    @Override
    public String getBody(Context ctx, Object... args)
    {
        return "";
    }
}
