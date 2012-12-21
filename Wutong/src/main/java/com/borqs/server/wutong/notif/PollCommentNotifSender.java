package com.borqs.server.wutong.notif;

import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.RecordSet;
import com.borqs.server.base.log.Logger;
import com.borqs.server.wutong.Constants;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.account2.AccountImpl;
import com.borqs.server.wutong.account2.AccountLogic;
import com.borqs.server.wutong.conversation.ConversationImpl;
import com.borqs.server.wutong.conversation.ConversationLogic;
import com.borqs.server.wutong.ignore.IgnoreImpl;
import com.borqs.server.wutong.ignore.IgnoreLogic;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class PollCommentNotifSender extends NotificationSender {
    private static final Logger L = Logger.getLogger(PollCommentNotifSender.class);
    private String displayNames = "";
    private String nameLinks = "";

    public PollCommentNotifSender() {
        super();
        isReplace = true;
    }

    protected List<Long> getScope(Context ctx, String senderId, Object... args) {
        L.debug(ctx, "=====NTF_POLL_COMMENT=====begin");
        List<Long> userIds = new ArrayList<Long>();
        List<Long> whoComments = new ArrayList<Long>();
        if (getSettingKey(ctx).equals(Constants.NTF_POLL_COMMENT)) {
                List<String> reasons = new ArrayList<String>();
                reasons.add(String.valueOf(Constants.C_POLL_CREATE));
                reasons.add(String.valueOf(Constants.C_POLL_COMMENT));
            ConversationLogic conversation = GlobalLogics.getConversation();
            RecordSet conversation_users = conversation.getConversation(ctx, Constants.POLL_OBJECT, (String) args[0], reasons, 0, 0, 100);
                for (Record r : conversation_users) {
                    if (!userIds.contains(Long.parseLong(r.getString("from_"))))
                        userIds.add(Long.parseLong(r.getString("from_")));
                }

                List<String> reasons1 = new ArrayList<String>();
                reasons1.add(String.valueOf(Constants.C_POLL_COMMENT));
                RecordSet conversation_users1 = conversation.getConversation(ctx, Constants.POLL_OBJECT, (String) args[0], reasons1, 0, 0, 100);
                for (Record r1 : conversation_users1) {
                    if (!whoComments.contains(Long.parseLong(r1.getString("from_"))))
                        whoComments.add(Long.parseLong(r1.getString("from_")));
                }

                List<String> reasons2 = new ArrayList<String>();
                reasons2.add(String.valueOf(Constants.C_COMMENT_ADDTO));
                RecordSet conversation_users_addto = conversation.getConversation(ctx, Constants.COMMENT_OBJECT, (String) args[2], reasons2, 0, 0, 100);
                for (Record r : conversation_users_addto) {
                    if (!userIds.contains(Long.parseLong(r.getString("from_"))))
                        userIds.add(Long.parseLong(r.getString("from_")));
                }

                //=========================new send to ,from conversation end ====================
            L.debug(ctx, "=====NTF_POLL_COMMENT=====join userIds"+StringUtils.join(userIds,","));
        }
        if (userIds.contains(Long.parseLong((String)args[1])))
            userIds.remove(Long.parseLong((String)args[1]));

        //exclude sender
        if(StringUtils.isNotBlank(senderId))
        {
            userIds.remove(Long.parseLong(senderId));
        }
        try {
            IgnoreLogic ignore = GlobalLogics.getIgnore();
            userIds = ignore.formatIgnoreUserListP(ctx, userIds, (String) args[0], "");
            whoComments = ignore.formatIgnoreUserListP(ctx, whoComments, (String) args[0], "");
        } catch (Exception e) {
        }
        L.debug(ctx, "=====NTF_POLL_COMMENT=====join userIds add ignore"+StringUtils.join(userIds,","));
            List<String> l = new ArrayList<String>();
            int size = whoComments.size();
            if (size > 4) {
                size = 4;
            }
            for (int i = 0; i < size; i++) {
                String userId = String.valueOf(whoComments.get(i));
                L.debug(ctx, "userId: " + userId);
                AccountLogic account = GlobalLogics.getAccount();
                String displayName = account.getUser(ctx, userId, userId, "display_name")
                        .getString("display_name", "");
                L.debug(ctx, "display name: " + displayName);

                displayNames += displayName + ", ";
                l.add("<a href=\"borqs://profile/details?uid=" + userId + "&tab=2\">" + displayName + "</a>");
            }

            nameLinks = StringUtils.join(l, ", ");
            if(StringUtils.isNotBlank(displayNames))
            {
                displayNames = StringUtils.substringBeforeLast(displayNames, ",");
            }
        L.debug(ctx, "=====NTF_POLL_COMMENT=====scope"+StringUtils.join(userIds,","));
        return userIds;
    }

    @Override
    protected String getSettingKey(Context ctx) {
        return Constants.NTF_POLL_COMMENT;
    }

    @Override
    protected String getAppId(Context ctx, Object... args) {
        return String.valueOf(Constants.APP_TYPE_BPC);
    }

    @Override
    protected String getTitle(Context ctx, Object... args) {
        String title = (String)args[0];

        return displayNames + " have commented the poll 【" + title + "】";
    }

    @Override
    protected String getUri(Context ctx, Object... args) {
        return "borqs://poll/comment?id=" + (String)args[0];
    }

    @Override
    protected String getTitleHtml(Context ctx, Object... args) {
        String title = (String) args[0];
        String pollId = (String) args[1];
        String pollSchema = "<a href=\"borqs://poll/details?id=" + pollId + "\">" + title + "</a>";

        return nameLinks + " have commented the poll 【" + pollSchema + "】";
    }

    @Override
    protected String getBody(Context ctx, Object... args)
	{
		return (String)args[0];
	}
}