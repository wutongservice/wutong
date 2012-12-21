package com.borqs.server.wutong.group;

import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.RecordSet;
import com.borqs.server.base.util.Initializable;
import com.borqs.server.wutong.notif.GroupNotificationSender;

import java.util.List;

public interface GroupLogic extends Initializable {
    long createGroup(Context ctx, long begin, String type, String name, int memberLimit, int isStreamPublic, int canSearch, int canViewMembers, int canJoin, int canMemberInvite, int canMemberApprove, int canMemberPost, int canMemberQuit, int needInvitedConfirm, long creator, String label, Record properties) ;
    long createGroup(Context ctx, long begin, String type, String name, int memberLimit, int isStreamPublic, int canSearch, int canViewMembers, int canJoin, int canMemberInvite, int canMemberApprove, int canMemberPost, int canMemberQuit, int needInvitedConfirm, long creator, String label, Record properties, boolean sendPost) ;
    boolean updateGroup(Context ctx, long groupId, Record info, Record properties) ;
    boolean destroyGroup(Context ctx, String groupIds) ;

    RecordSet getSimpleGroups(Context ctx, long begin, long end, String groupIds, String cols) ;
    RecordSet getGroups(Context ctx, long begin, long end, String userId, String groupIds, String cols, boolean withMembers) ;
    Record getGroup(Context ctx, String userId, long groupId, String cols, boolean withMembers);
    RecordSet findGroupsByMember(Context ctx, long begin, long end, long member, String cols) ;
    String findGroupIdsByMember(Context ctx, long begin, long end, long member) ;
    String findGroupIdsByTopPost(Context ctx, String postId) ;
    RecordSet findGroupsByName(Context ctx, long begin, long end, String name, String cols) ;

    boolean addSimpleMember(Context ctx, long groupId, long member, int role) ;
    boolean addSimpleMembers(Context ctx, long groupId, Record roles) ;
    int addMember(Context ctx, long groupId, String userId, String message) ;
    int addMember(Context ctx, long groupId, String userId, String message, boolean sendPost) ;
    RecordSet addMembers(Context ctx, long groupId, Record roles, boolean sendPost) ;
    boolean removeMembers(Context ctx, long groupId, String members, String newAdmins) ;

    boolean grants(Context ctx, long groupId, Record roles) ;
    String getMembersByRole(Context ctx, long groupId, int role, int page, int count, String searchKey) ;
    String getAdmins(Context ctx, long groupId, int page, int count) ;
    long getCreator(Context ctx, long groupId) ;
    String getAllMembers(Context ctx, long groupId, int page, int count, String searchKey) ;
    String getMembers(Context ctx, String groupIds, int page, int count) ;
    int getMembersCount(Context ctx, long groupId) ;
    Record getMembersCounts(Context ctx, String groupIds) ;
    boolean hasRight(Context ctx, long groupId, long member, int minRole) ;

    boolean addOrUpdatePendings(Context ctx, long groupId, RecordSet statuses) ;
    RecordSet getPendingUsersByStatus(Context ctx, long groupId, long source, String status, int page, int count, String searchKey) ;
    int getUserStatusById(Context ctx, long groupId, long userId) ;
    int getUserStatusByIdentify(Context ctx, long groupId, String identify) ;
    Record getUserStatusByIds(Context ctx, long groupId, String userIds) ;
    Record getUserStatusByIdentifies(Context ctx, long groupId, String identifies) ;
    Record getUsersCounts(Context ctx, String groupIds, int status) ;
    boolean updateUserIdByIdentify(Context ctx, String userId, String identify) ;

    String getSourcesById(Context ctx, long groupId, String userId) ;
    String getSourcesByIdentify(Context ctx, long groupId, String identify) ;

    boolean isGroup(Context ctx, long id) ;
    boolean isPublicCircle(Context ctx, long id) ;
    boolean isActivity(Context ctx, long id) ;
    boolean isOrganization(Context ctx, long id) ;
    boolean isGeneralGroup(Context ctx, long id) ;
    boolean isEvent(Context ctx, long id) ;
    boolean isSpecificType(Context ctx, long id, String type) ;

    boolean defaultMemberNotification(Context ctx, long groupId, String userIds) ;
    boolean updateMemberNotification(Context ctx, long groupId, String userId, Record notif) ;
    RecordSet getMembersNotification(Context ctx, long groupId, String userIds) ;
    RecordSet  getGroupUsersByStatus(Context ctx, long groupId, String status, int page, int count, String searchKey) ;

    RecordSet getCompatibleGroups(Context ctx, String viewerId, String groupIds) ;
    int getTypeByStr(Context ctx, String str);
    RecordSet inviteMembers(Context ctx, long groupId, String userIds, String message) ;
    Record inviteMember(Context ctx, long groupId, String to, String name, String message);
    String getCreatorAndAdmins(Context ctx, long groupId) ;
    String getGroupTypeStr(Context ctx, long groupId) ;
    Record approveMember(Context ctx, long groupId, String userId) ;
    Record ignoreMember(Context ctx, long groupId, String userId) ;
    int dealGroupInvite(Context ctx, long groupId, String userId, String source, boolean accept);
    int rejectGroupInviteForIdentify(Context ctx, long groupId, String name, String identify, String source);
    RecordSet searchGroups(Context ctx, long begin, long end, String name, String userId, String cols);
    long getBeginByGroupType(Context ctx, String type);
    long getEndByGroupType(Context ctx, String type);
    String canApproveUsers(Context ctx, long groupId);
    void sendGroupNotification(Context ctx, long groupId, GroupNotificationSender sender,
                               String senderId, Object[] scopeArgs, String bodyArg, String... titleArgs);

    boolean getUserAndGroup(Context ctx, StringBuilder retMentions, final String mentions, List<String> groupIds);
    List<String> getGroupIdsFromMentions(Context ctx, List<String> mentions);
    public RecordSet dealWithGroupFile(Context ctx, Record viewerFileRec, List<String> groupIds);

    String findGroupIdsByProperty(Context ctx, String propKey, String propVal, int max);
}
