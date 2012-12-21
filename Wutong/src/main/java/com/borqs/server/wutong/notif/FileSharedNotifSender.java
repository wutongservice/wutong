package com.borqs.server.wutong.notif;

import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.RecordSet;
import com.borqs.server.base.util.StringUtils2;
import com.borqs.server.wutong.Constants;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.conversation.ConversationImpl;
import com.borqs.server.wutong.conversation.ConversationLogic;
import com.borqs.server.wutong.group.GroupImpl;
import com.borqs.server.wutong.group.GroupLogic;
import com.borqs.server.wutong.ignore.IgnoreImpl;
import com.borqs.server.wutong.ignore.IgnoreLogic;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileSharedNotifSender extends GroupNotificationSender {
    private ArrayList<Long> groups = new ArrayList<Long>();

    public FileSharedNotifSender() {
        super();
        isReplace = true;
    }
    
    @Override
	public List<Long> getScope(Context ctx, String senderId, Object... args) {
		List<Long> userIds = new ArrayList<Long>();

            List<String> reasons = new ArrayList<String>();
            reasons.add(String.valueOf(Constants.C_STREAM_TO));
            reasons.add(String.valueOf(Constants.C_STREAM_ADDTO));
        ConversationLogic conversation = GlobalLogics.getConversation();
        RecordSet conversation_users = conversation.getConversation(ctx, Constants.POST_OBJECT, (String) args[0], reasons, 0, 0, 100);
            for (Record r : conversation_users) {
                long userId = Long.parseLong(r.getString("from_"));
                if (!userIds.contains(userId)) {
                    if ((userId >= Constants.PUBLIC_CIRCLE_ID_BEGIN)
                            && (userId <= Constants.GROUP_ID_END)) {
                        groups.add(userId);
                        GroupLogic groupImpl = GlobalLogics.getGroup();
                        String members = groupImpl.getAllMembers(ctx, userId, -1, -1, "");
                        List<Long> memberIds = StringUtils2.splitIntList(members, ",");
                        userIds.addAll(memberIds);
                    }
                    else
                        userIds.add(userId);
                }
            }
            //=========================new send to ,from conversation end ====================

        HashSet<Long> set = new HashSet<Long>(userIds);
        userIds = new ArrayList<Long>(set);
		//exclude sender
        if(StringUtils.isNotBlank(senderId))
        {
        	userIds.remove(Long.parseLong(senderId));
        }
		try {
            IgnoreLogic ignore = GlobalLogics.getIgnore();
            userIds = ignore.formatIgnoreUserListP(ctx, userIds, "", "");
        } catch (Exception e) {
        }
		return userIds;
	}
    
   @Override
    protected String getSettingKey(Context ctx) {
        return Constants.NTF_FILE_SHARE;
    }

    @Override
    protected String getAppId(Context ctx, Object... args) {
        String sType = (String)args[0];
//    	return String.valueOf(findAppIdFromPostType((Integer)args[0]));
        return String.valueOf(findAppIdFromPostType(ctx, Integer.parseInt(sType)));
    }

    public String getFileType(Context ctx, String type){
         String return_type="文件";
        if (type.equals(String.valueOf(Constants.AUDIO_POST)))
            return_type = "音频";
         if (type.equals(String.valueOf(Constants.VIDEO_POST)))
            return_type = "视频";
        return return_type;
    }
    
    @Override
    public String getTitle(Context ctx, Object... args) {
        if (groups.isEmpty())    {
            return args[1] + "给您分享了他的"+getFileType(ctx, String.valueOf(args[0]))+":"+String.valueOf(args[2])+"";
        }
        else {
            String groupIds = StringUtils2.joinIgnoreBlank(",", groups);
            String groupType = "公共圈子";
            RecordSet recs = new RecordSet();

            GroupLogic groupImpl = GlobalLogics.getGroup();
                groupType = groupImpl.getGroupTypeStr(ctx, groups.get(0));
                recs = groupImpl.getSimpleGroups(ctx, Constants.PUBLIC_CIRCLE_ID_BEGIN, Constants.GROUP_ID_END, groupIds, Constants.GRP_COL_NAME);

            ArrayList<String> groupNames = new ArrayList<String>();
            for (Record rec : recs) {
                String groupName = rec.getString(Constants.GRP_COL_NAME);
                groupNames.add("【" + groupName + "】");
            }
            return args[1] + "在" + groupType + StringUtils2.joinIgnoreBlank("，", groupNames) + "分享了他的"+getFileType(ctx, String.valueOf(args[0]))+":"+String.valueOf(args[2])+"";
        }
    }

    @Override
    protected String getUri(Context ctx, Object... args) {
        Object postId = args[0];
        return "borqs://stream/comment?id=" + postId;
    }
    
    @Override
    protected String getTitleHtml(Context ctx, Object... args) {
        if (groups.isEmpty())
            return "<a href=\"borqs://profile/details?uid=" + args[1] + "&tab=2\">" + args[2]+ "</a>给您分享了他的"+getFileType(ctx, String.valueOf(args[0]))+":"+String.valueOf(args[3])+"";
        else {
            String groupIds = StringUtils2.joinIgnoreBlank(",", groups);
            String groupType = "公共圈子";
            RecordSet recs = new RecordSet();

            GroupLogic groupImpl = GlobalLogics.getGroup();
            groupType = groupImpl.getGroupTypeStr(ctx, groups.get(0));
                recs = groupImpl.getSimpleGroups(ctx, Constants.PUBLIC_CIRCLE_ID_BEGIN, Constants.GROUP_ID_END, groupIds, Constants.GRP_COL_NAME + "," + Constants.GRP_COL_ID);

            ArrayList<String> groupNames = new ArrayList<String>();
            for (Record rec : recs) {
                String groupName = rec.getString(Constants.GRP_COL_NAME);
                String groupId = rec.getString(Constants.GRP_COL_ID);
                groupNames.add("【<a href=\"borqs://profile/details?uid=" + groupId + "&tab=2\">" + groupName + "</a>】");
            }
            return "<a href=\"borqs://profile/details?uid=" + args[1] + "&tab=2\">" + args[2] + "</a>在" + groupType + StringUtils2.joinIgnoreBlank("，", groupNames) + "分享了他的"+getFileType(ctx, String.valueOf(args[0]))+":"+String.valueOf(args[2])+"";
        }
    }
    
    @Override
    public String getBody(Context ctx, Object... args)
	{
		return (String)args[0];
	}
}