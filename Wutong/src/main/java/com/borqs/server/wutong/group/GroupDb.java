package com.borqs.server.wutong.group;

import com.borqs.server.ServerException;
import com.borqs.server.base.conf.Configuration;
import com.borqs.server.base.conf.GlobalConfig;
import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.RecordSet;
import com.borqs.server.base.data.Schema;
import com.borqs.server.base.data.Schemas;
import com.borqs.server.base.log.Logger;
import com.borqs.server.base.sql.*;
import com.borqs.server.base.util.DateUtils;
import com.borqs.server.base.util.StringUtils2;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.borqs.server.wutong.Constants.*;
import static com.borqs.server.wutong.WutongErrors.GROUP_CREATE_ERROR;

public class GroupDb {
    private static final Logger L = Logger.getLogger(GroupDb.class);
    protected final Schema groupSchema = Schema.loadClassPath(GroupDb.class, "group.schema");
    protected final Schema notifSchema = Schema.loadClassPath(GroupDb.class, "notif.schema");

    private ConnectionFactory connectionFactory;
    private String db;
    private String groupTable = "group_";
    private String groupPropertyTable = "group_property";
    private String groupMembersTable = "group_members";
    private String groupPendingsTable = "group_pendings";
    private String globalCounterTable;

    public GroupDb() {
        Configuration conf = GlobalConfig.get();
        groupSchema.loadAliases(conf.getString("schema.group.alias", null));

        this.connectionFactory = ConnectionFactory.getConnectionFactory(conf.getString("group.simple.connectionFactory", "dbcp"));
        this.db = conf.getString("group.simple.db", null);
        this.groupTable = conf.getString("group.simple.groupTable", "group_");
        this.groupPropertyTable = conf.getString("group.simple.groupPropertyTable", "group_property");
        this.groupMembersTable = conf.getString("group.simple.groupMembersTable", "group_members");
        this.groupPendingsTable = conf.getString("group.simple.groupPendingsTable", "group_pendings");
        this.globalCounterTable = conf.getString("group.simple.globalCounterTable", "user_id_counter");
    }

    @Override
    protected void finalize() {
        this.groupTable = null;
        this.groupMembersTable = null;
        this.globalCounterTable = null;
        this.connectionFactory = ConnectionFactory.close(connectionFactory);
        db = null;
    }


    private SQLExecutor getSqlExecutor() {
        return new SQLExecutor(connectionFactory, db);
    }


    protected boolean defaultMemberNotification0(Context ctx, long groupId, String userIds) {
        if (StringUtils.isBlank(userIds))
            return false;

        String sql1 = "SELECT user_id, login_email1, login_phone1 FROM user2 WHERE user_id IN (" + userIds + ")";
        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql1, null);

        ArrayList<String> sqls = new ArrayList<String>();
        for (Record rec : recs) {
            String userId = rec.getString("user_id");
            String loginEmail1 = rec.getString("login_email1", "");
//            String loginPhone1 = rec.getString("login_phone1", "");
            String loginPhone1 = "";
            if (StringUtils.isBlank(loginEmail1))
                loginEmail1 = "";
            if (StringUtils.isBlank(loginPhone1))
                loginPhone1 = "";

            int recvNotif = (groupId >= EVENT_ID_BEGIN && groupId < EVENT_ID_END) ? 1 : 0;
            String sql2 = "UPDATE " + groupMembersTable + " SET recv_notif=" + recvNotif + ", notif_email='" + loginEmail1 + "', notif_phone='" + loginPhone1
                    + "' WHERE group_id=" + groupId + " AND member=" + userId;
            sqls.add(sql2);
        }
        long n = se.executeUpdate(sqls);
        return n > 0;
    }

    protected boolean updateMemberNotification0(Context ctx, long groupId, String userId, Record notif) {
        String sql = new SQLBuilder.Update(notifSchema)
                .update(groupMembersTable)
                .values(notif)
                .where("group_id=" + groupId)
                .and("member=" + userId)
                .toString();

        SQLExecutor se = getSqlExecutor();
        long n = se.executeUpdate(sql);
        return n > 0;
    }

    protected RecordSet getMembersNotification0(Context ctx, long groupId, String userIds) {
        RecordSet recs = new RecordSet();

        if (StringUtils.isNotBlank(userIds)) {
            String sql = "SELECT member, recv_notif, notif_email, notif_phone FROM " + groupMembersTable
                    + " WHERE group_id=" + groupId + " AND member IN (" + userIds + ")";
            SQLExecutor se = getSqlExecutor();
            recs = se.executeRecordSet(sql, recs);

            String sql2 = "SELECT user_id, login_email1, login_email2, login_email3, login_phone1, login_phone2, " +
                    "login_phone3 FROM user2 WHERE user_id IN (" + userIds + ")";
            RecordSet recs2 = se.executeRecordSet(sql2, null);

            recs = recs.mergeByKeys("member", recs2, "user_id", null);
        }

        return recs;
    }

    protected boolean saveGroup(Context ctx, Record info, Record properties) {
        Schemas.standardize(groupSchema, info);

        final String SQL = "INSERT INTO ${table} ${values_join(alias, info)}";
        String sql1 = SQLTemplate.merge(SQL,
                "table", groupTable,
                "alias", groupSchema.getAllAliases(),
                "info", info);

        SQLExecutor se = getSqlExecutor();
        long n1 = se.executeUpdate(sql1);
        boolean r = n1 > 0;

        long groupId = info.getInt(GRP_COL_ID);
        if (r && !properties.isEmpty()) {
            String sql2 = "INSERT INTO " + groupPropertyTable + " (`group_id`, `key_`, `value_`) VALUES ";

            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                String key = entry.getKey();
                String value = ObjectUtils.toString(entry.getValue());
                sql2 += "(" + groupId + ", '" + key + "', '" + value + "'), ";
            }
            sql2 = StringUtils.substringBeforeLast(sql2, ",");

            long n2 = se.executeUpdate(sql2);
            r = r && n2 > 0;
        }

        long now = DateUtils.nowMillis();
        String sql3 = "INSERT INTO " + groupMembersTable + " (group_id, member, role, joined_time, updated_time, created_time) VALUES ("
                + groupId + ", " + info.getInt(GRP_COL_CREATOR) + ", " + ROLE_CREATOR + ", " + now + ", " + now + ", " + now + ")";
        long n3 = se.executeUpdate(sql3);
        r = r && n3 > 0;

        boolean b = defaultMemberNotification0(ctx, groupId, info.getString(GRP_COL_CREATOR));
        r = r && b;

        return r;
    }

    protected long generateGroupId(Context ctx, long begin, String type) {
        L.trace(null, "begin: " + begin);
        L.trace(null, "activity: " + ACTIVITY_ID_BEGIN);
        L.trace(null, "compare: " + (begin == ACTIVITY_ID_BEGIN));

        if (begin == ACTIVITY_ID_BEGIN)
            type = TYPE_ACTIVITY;
        else if (begin == DEPARTMENT_ID_BEGIN)
            type = TYPE_DEPARTMENT;
        else if (begin == PUBLIC_CIRCLE_ID_BEGIN)
            type = TYPE_PUBLIC_CIRCLE;
        else if (begin == GENERAL_GROUP_ID_BEGIN)
            type = TYPE_GENERAL_GROUP;
        else if (begin == EVENT_ID_BEGIN)
            type = TYPE_EVENT;

        String SQL1 = "INSERT INTO ${table} (key_, count_) VALUES ('${type}', 0)"
                + " ON DUPLICATE KEY UPDATE count_ = count_ + 1";

        String SQL2 = "SELECT count_ FROM ${table} WHERE key_ = '${type}'";

        String sql1 = SQLTemplate.merge(SQL1, "table", globalCounterTable, "type", type);
        String sql2 = SQLTemplate.merge(SQL2, "table", globalCounterTable, "type", type);

        SQLExecutor se = getSqlExecutor();
        se.executeUpdate(sql1);
        Record rec = se.executeRecord(sql2, null);
        long count = rec.getInt("count_", -1);
        if (count == -1)
            throw new ServerException(GROUP_CREATE_ERROR, "Generate group id error");

        return begin + count;
    }

    protected boolean updateGroup0(Context ctx, long groupId, Record info, Record properties) {
        String sql1 = new SQLBuilder.Update(groupSchema)
                .update(groupTable)
                .values(info)
                .where("${alias.id}=${v(id)}", "id", groupId)
                .and("destroyed_time=0")
                .toString();

        SQLExecutor se = getSqlExecutor();
        long n1 = se.executeUpdate(sql1);
        boolean r = n1 > 0;

        if (r && !properties.isEmpty()) {
            String sql2 = "INSERT INTO " + groupPropertyTable + " (`group_id`, `key_`, `value_`) VALUES ";

            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                String key = entry.getKey();
                String value = ObjectUtils.toString(entry.getValue());
                sql2 += "(" + groupId + ", '" + key + "', '" + value + "'), ";
            }
            sql2 = StringUtils.substringBeforeLast(sql2, ",");
            sql2 += " ON DUPLICATE KEY UPDATE value_=VALUES(value_)";

            long n2 = se.executeUpdate(sql2);
            r = r && n2 > 0;
        }

        return r;
    }

    protected boolean deleteGroups(Context ctx, String groupIds) {
        final String SQL = "UPDATE ${table} SET destroyed_time=${v(deleted_time)} WHERE ${alias.id} IN (${ids})";
        String sql = SQLTemplate.merge(SQL,
                "table", groupTable,
                "deleted_time", DateUtils.nowMillis(),
                "alias", groupSchema.getAllAliases(),
                "ids", groupIds);

        SQLExecutor se = getSqlExecutor();
        long n = se.executeUpdate(sql);

        return n > 0;
    }

    private RecordSet fillGroupsProperties(Context ctx, RecordSet recs, String... cols) {
        String SQL = "SELECT `key_`, `value_` FROM ${table} WHERE `group_id`=${groupId}";
        if (ArrayUtils.isNotEmpty(cols))
            SQL += " AND `key_` IN (" + SQLUtils.valueJoin(",", cols) + ")";

        for (Record rec : recs) {
            String sql = SQLTemplate.merge(SQL,
                    "table", groupPropertyTable,
                    "groupId", rec.getString(GRP_COL_ID));

            SQLExecutor se = getSqlExecutor();
            RecordSet kvSet = se.executeRecordSet(sql, null);

            for (Record kv : kvSet) {
                rec.put(kv.getString("key_"), kv.getString("value_"));
            }
        }

        return recs;
    }

    private String filter(long begin, long end) {
        String condition = "";
        if (begin >= PUBLIC_CIRCLE_ID_BEGIN
                && end <= GROUP_ID_END
                && end >= begin) {
            condition = " AND id>=" + begin + " AND id<" + end;
        }

        return condition;
    }

    protected RecordSet getGroups(Context ctx, long begin, long end, long[] groupIds, String... cols) {
        String SQL = "SELECT ${as_join(alias)} FROM ${table} WHERE ${alias.id} IN (${ids}) AND destroyed_time=0";
        if (groupIds.length == 1)
            SQL = "SELECT ${as_join(alias)} FROM ${table} WHERE ${alias.id}=${ids} AND destroyed_time=0";
        String sql = SQLTemplate.merge(SQL,
                "alias", groupSchema.getAllAliases(),
                "table", groupTable,
                "ids", groupIds.length == 1 ? String.valueOf(groupIds[0]) : StringUtils2.join(groupIds, ","));

        sql += filter(begin, end);

        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        Schemas.standardize(groupSchema, recs);

        fillGroupsProperties(ctx, recs, cols);
        return recs;
    }

    protected RecordSet findGroupsByMember0(Context ctx, long begin, long end, long member, String... cols) {
        final String SQL0 = "SELECT group_id FROM ${groupMembersTable} WHERE member=${member} AND destroyed_time=0";
        final String SQL = "SELECT ${as_join(alias)} FROM ${groupTable} WHERE ${alias.id} IN (" + SQL0  + ") AND destroyed_time=0";

        String sql = SQLTemplate.merge(SQL,
                "alias", groupSchema.getAllAliases(),
                "groupTable", groupTable,
                "groupMembersTable", groupMembersTable,
                "member", String.valueOf(member));

        sql += filter(begin, end);
        sql += " order by created_time desc";

        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        Schemas.standardize(groupSchema, recs);

        fillGroupsProperties(ctx, recs, cols);
        return recs;
    }

    protected String findGroupIdsByMember0(Context ctx, long begin, long end, long member) {
        final String SQL0 = "SELECT group_id FROM ${groupMembersTable} WHERE member=${member} AND destroyed_time=0";
        final String SQL = "SELECT ${alias.id} FROM ${groupTable} WHERE ${alias.id} IN (" + SQL0  + ") AND destroyed_time=0";

        String sql = SQLTemplate.merge(SQL,
                "alias", groupSchema.getAllAliases(),
                "groupTable", groupTable,
                "groupMembersTable", groupMembersTable,
                "member", String.valueOf(member));

        sql += filter(begin, end);

        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        ArrayList<Long> groupIds = new ArrayList<Long>();
        for (Record rec : recs) {
            groupIds.add(rec.getInt("id"));
        }

        return StringUtils2.joinIgnoreBlank(",", groupIds);
    }

    protected RecordSet findGroupsByName0(Context ctx, long begin, long end, String name, String... cols) {
        final String SQL = "SELECT ${as_join(alias)} FROM ${table} WHERE ${alias.name} like '%${name}%' AND destroyed_time=0";
        String sql = SQLTemplate.merge(SQL,
                "alias", groupSchema.getAllAliases(),
                "table", groupTable,
                "name", name);

        sql += filter(begin, end);

        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        Schemas.standardize(groupSchema, recs);

        fillGroupsProperties(ctx, recs, cols);
        return recs;
    }

    protected boolean addOrGrantMembers(Context ctx, long groupId, Record memberRoles) {
        String sql0 = "SELECT member_limit FROM group_ WHERE id=" + groupId;
        SQLExecutor se = getSqlExecutor();
        long memberLimit = se.executeIntScalar(sql0, 100);
        int memberCount = getMembersCount0(ctx, groupId);
        long n = 0;

        if (memberCount < memberLimit) {
            String sql = "INSERT INTO " + groupMembersTable + " (group_id, member, role, joined_time, updated_time, created_time) VALUES ";

            long now = DateUtils.nowMillis();
            for (Map.Entry<String, Object> entry : memberRoles.entrySet()) {
                long member = Long.parseLong(entry.getKey());
                int role = ((Number) entry.getValue()).intValue();
                sql += "(" + groupId + ", " + member + ", " + role + ", " + now + ", " + now + ", " + now + "), ";
            }

            sql = StringUtils.substringBeforeLast(sql, ",");
            sql += " ON DUPLICATE KEY UPDATE role=VALUES(role), joined_time=VALUES(joined_time), updated_time=VALUES(updated_time), destroyed_time=0";

            n = se.executeUpdate(sql);
        }

        return n > 0;
    }

    protected boolean deleteMembers(Context ctx, long groupId, String members) {
        final String SQL = "UPDATE ${table} SET destroyed_time=${v(deleted_time)} WHERE group_id=${groupId} AND member IN (${members})";
        String sql = SQLTemplate.merge(SQL,
                "table", groupMembersTable,
                "deleted_time", DateUtils.nowMillis(),
                "groupId", String.valueOf(groupId),
                "members", members);

        SQLExecutor se = getSqlExecutor();
        long n = se.executeUpdate(sql);

        return n > 0;
    }

    protected boolean hasRight0(Context ctx, long groupId, long member, int minRole) {
        String sql = "SELECT role FROM " + groupMembersTable + " WHERE group_id=" + groupId + " AND member=" + member + " AND destroyed_time=0";
        SQLExecutor se = getSqlExecutor();
        int role = (int) se.executeIntScalar(sql, 0);
        return role >= minRole;
    }

    protected String getMembersByRole0(Context ctx, long groupId, int role, int page, int count, String searchKey) {
        String sql = "SELECT member FROM " + groupMembersTable + " WHERE group_id=" + groupId;
        if (role != 0)
            sql += " AND role=" + role;
        sql += " AND destroyed_time=0";

        if (StringUtils.isNotBlank(searchKey)) {
            String sql0 = "SELECT user_id FROM user2 where (display_name like '%" + searchKey + "%' or sort_key like '%" + searchKey + "%') AND destroyed_time=0";
            sql += " AND member IN (" + sql0 + ")";
        }

        sql += " order by joined_time desc " + SQLTemplate.merge(" ${limit}",
                "limit", SQLUtils.pageToLimit(page, count));

        SQLExecutor se = getSqlExecutor();
        if (role == ROLE_CREATOR) {
            long member = se.executeIntScalar(sql, 0);
            return ObjectUtils.toString(member);
        } else {
            RecordSet recs = se.executeRecordSet(sql, null);
            ArrayList<Long> members = new ArrayList<Long>();
            for (Record rec : recs) {
                members.add(rec.getInt("member"));
            }

            return StringUtils2.joinIgnoreBlank(",", members);
        }
    }

    protected String getMembers0(Context ctx, String groupIds, int page, int count) {
        String sql = "SELECT member FROM " + groupMembersTable + " WHERE group_id IN (" + groupIds + ") AND destroyed_time=0";

        sql += SQLTemplate.merge(" ${limit}",
                "limit", SQLUtils.pageToLimit(page, count));

        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        ArrayList<Long> members = new ArrayList<Long>();
        for (Record rec : recs) {
            members.add(rec.getInt("member"));
        }

        return StringUtils2.joinIgnoreBlank(",", members);

    }

    protected int getMembersCount0(Context ctx, long groupId) {
        String sql = "SELECT count(*) FROM " + groupMembersTable + " WHERE group_id=" + groupId + " AND destroyed_time=0";
        SQLExecutor se = getSqlExecutor();
        return (int) se.executeIntScalar(sql, 0);
    }

    protected Record getMembersCounts0(Context ctx, String groupIds) {
        String sql = "SELECT group_id, count(*) AS count FROM " + groupMembersTable + " WHERE group_id in (" + groupIds + ") AND destroyed_time=0 group by group_id";
        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);

        Record r = new Record();
        for (Record rec : recs)
            r.put(rec.getString("group_id"), rec.getInt("count", 0));
        return r;
    }

    protected Record getUsersCounts0(Context ctx, String groupIds, int status) {
        if (StringUtils.isBlank(groupIds))
            return new Record();

        String sql1 = "SELECT group_id, count(DISTINCT user_id) AS count FROM " + groupPendingsTable + " WHERE group_id in (" + groupIds + ") AND status="
                + status + " AND user_id <> 0 group by group_id";
        String sql2 = "SELECT group_id, count(DISTINCT identify) AS count FROM " + groupPendingsTable + " WHERE group_id in (" + groupIds + ") AND status="
                + status + " AND identify <> '' AND user_id=0 group by group_id";
        SQLExecutor se = getSqlExecutor();
        RecordSet recs1 = se.executeRecordSet(sql1, null);
        RecordSet recs2 = se.executeRecordSet(sql2, null);

        Record r = new Record();
        for (Record rec : recs1)
            r.put(rec.getString("group_id"), rec.getInt("count", 0));
        for (Record rec : recs2) {
            String groupId = rec.getString("group_id");
            long count2 = rec.getInt("count", 0);
            long count1 = r.getInt(groupId);
            r.put(groupId, count1 + count2);
        }

        return r;
    }

    protected boolean addOrUpdatePendings0(Context ctx, long groupId, RecordSet statuses) {
        ArrayList<String> sqls = new ArrayList<String>();

        long now = DateUtils.nowMillis();
        for (Record status : statuses) {
            String sql = "INSERT INTO " + groupPendingsTable + " (group_id, user_id, display_name, identify, source, status, created_time, updated_time, destroyed_time) VALUES ";
            long userId = status.getInt("user_id", 0);
            String displayName = status.getString("display_name", "");
            String identify = status.getString("identify", "");
            long source = status.getInt("source", 0);
            int status_ = (int) status.getInt("status");
            long destroyedTime = 0;
            if ((status_ == STATUS_JOINED) || (status_ == STATUS_REJECTED)
                    || (status_ == STATUS_KICKED) || (status_ == STATUS_QUIT))
                destroyedTime = now;
            sql += "(" + groupId + ", " + userId + ", '" + displayName + "', '" + identify + "', " + source + ", " + status_ + ", " + now + ", " + now + ", " + destroyedTime + ")";
            sql += " ON DUPLICATE KEY UPDATE status=VALUES(status), updated_time=VALUES(updated_time), destroyed_time=VALUES(destroyed_time)";
            sqls.add(sql);

            sql = "UPDATE " + groupPendingsTable + " SET status=" + status_ + ", updated_time=" + now + ", destroyed_time=" + destroyedTime
                    + " WHERE group_id=" + groupId;
            if (userId != 0)
                sql += " AND user_id=" + userId;
            else
                sql += " AND identify='" + identify + "'";
            sqls.add(sql);
        }

        SQLExecutor se = getSqlExecutor();
        long n = se.executeUpdate(sqls);
        return n > 0;
    }

    protected RecordSet getPendingUserByStatus0(Context ctx, long groupId, long source, String status, int page, int count, String searchKey) {
        String sql = "SELECT * FROM " + groupPendingsTable + " WHERE group_id=" + groupId;

        if (StringUtils.isNotBlank(status))
            sql += " AND status IN (" + status + ")";

        if (source > 0) {
            sql += " AND source=" + source;
        }

        if (StringUtils.isNotBlank(searchKey)) {
            String sql0 = "SELECT user_id FROM user2 where (display_name like '%" + searchKey + "%' or sort_key like '%" + searchKey + "%') AND destroyed_time=0";
            sql += " AND user_id IN (" + sql0 + ")";
        }

        sql += SQLTemplate.merge(" ${limit}",
                "limit", SQLUtils.pageToLimit(page, count));

        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        return recs;
    }

    /**
     * union the record of groupUsers
     * @param groupId
     * @param status
     * @param page
     * @param count
     * @param searchKey
     * @return
     */
    public RecordSet getGroupUsersByStatus0(Context ctx, long groupId, String status, int page, int count, String searchKey) {
        String sql = "SELECT member AS user_id,'0' identify ,'group_members' AS t, '' as role, null AS display_name,null AS source ,null AS status, created_time," +
                " updated_time,destroyed_time, null AS group_id FROM " + groupMembersTable + " WHERE group_id=" + groupId;
        sql += " AND destroyed_time=0";

        String sql2 = "SELECT user_id AS user_id, identify , 'group_pendings' AS t, '' AS role ,  display_name, source ,status, created_time," +
                " updated_time,destroyed_time, group_id FROM " + groupPendingsTable + " WHERE group_id=" + groupId;
        sql2 += " AND destroyed_time=0";

        String sqlUnion = "";

        if (StringUtils.isNotBlank(status)){
            sql2 += " AND status IN (" + status + ")";

            sqlUnion= "SELECT a.* FROM (" + sql + " union " + sql2 + ") as a where 1 = 1 ";
        }else{
            sqlUnion= "SELECT a.* FROM (" + sql +  ") as a where 1 = 1 ";
        }

        String sql0 = "SELECT user_id FROM user2 where  destroyed_time=0";
        if (StringUtils.isNotBlank(searchKey)) {
            sql0 = "SELECT user_id FROM user2 where (display_name like '%" + searchKey + "%' or sort_key like '%" + searchKey + "%') AND destroyed_time=0";
        }
        sqlUnion += " AND a.user_id IN (" + sql0 + ")";

        sqlUnion += "ORDER BY role DESC " + SQLTemplate.merge(" ${limit}",
                "limit", SQLUtils.pageToLimit(page, count));
        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sqlUnion, null);
        return recs;
    }

    protected int getUserStatusById0(Context ctx, long groupId, long userId) {
        String sql = "SELECT status FROM " + groupPendingsTable + " WHERE user_id=" + userId + " AND group_id=" + groupId;
        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        if (recs.isEmpty())
            return 0;
        else
            return (int) recs.getFirstRecord().getInt("status", 0L);
    }

    protected int getUserStatusByIdentify0(Context ctx, long groupId, String identify) {
        String sql = "SELECT status FROM " + groupPendingsTable + " WHERE identify='" + identify + "' AND group_id=" + groupId;
        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        if (recs.isEmpty())
            return 0;
        else
            return (int) recs.getFirstRecord().getInt("status", 0L);
    }

    protected Record getUserStatusByIds0(Context ctx, long groupId, String userIds) {
        Record r = new Record();
        if (StringUtils.isNotBlank(userIds)) {
            String sql = "SELECT user_id, status FROM " + groupPendingsTable + " WHERE user_id IN (" + userIds + ") AND group_id=" + groupId;
            SQLExecutor se = getSqlExecutor();
            RecordSet recs = se.executeRecordSet(sql, null);

            List<String> l = StringUtils2.splitList(userIds, ",", true);
            for (String userId : l)
                r.put(userId, 0L);

            for (Record rec : recs)
                r.put(rec.getString("user_id"), rec.getInt("status", 0L));

        }

        return r;
    }

    protected Record getUserStatusByIdentifies0(Context ctx, long groupId, String identifies) {
        Record r = new Record();
        if (StringUtils.isNotBlank(identifies)) {
            String s = SQLUtils.valueJoin(",", StringUtils2.splitArray(identifies, ",", true));
            String sql = "SELECT identify, status FROM " + groupPendingsTable + " WHERE identify IN (" + s + ") AND group_id=" + groupId;
            SQLExecutor se = getSqlExecutor();
            RecordSet recs = se.executeRecordSet(sql, null);

            List<String> l = StringUtils2.splitList(identifies, ",", true);
            for (String identify : l)
                r.put(identify, 0L);

            for (Record rec : recs)
                r.put(rec.getString("identify"), rec.getInt("status", 0L));
        }

        return r;
    }

    protected boolean updateUserIdByIdentify0(Context ctx, String userId, String identify) {
        String sql = "UPDATE " + groupPendingsTable + " SET user_id=" + userId + " WHERE identify='" + identify + "'";
        SQLExecutor se = getSqlExecutor();
        long n = se.executeUpdate(sql);
        return n > 0;
    }

    protected String getSourcesById0(Context ctx, long groupId, String userId) {
        String sql = "SELECT source FROM " + groupPendingsTable + " WHERE user_id=" + userId + " AND group_id=" + groupId;
        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        return recs.joinColumnValues("source", ",");
    }

    protected String getSourcesByIdentify0(Context ctx, long groupId, String identify) {
        String sql = "SELECT source FROM " + groupPendingsTable + " WHERE identify='" + identify + "' AND group_id=" + groupId;
        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        return recs.joinColumnValues("source", ",");
    }

    protected String findGroupIdsByTopPost0(Context ctx, String postId) {
        String sql = "SELECT group_id FROM " + groupPropertyTable + " WHERE key_='" + COMM_COL_TOP_POSTS
                + "' AND instr(value_, '" + postId + "')>0";
        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        String groupIds = recs.joinColumnValues("group_id", ",");

        sql = "SELECT user FROM user_property WHERE `key`=28 AND instr(`value`, '" + postId + "')>0";
        recs = se.executeRecordSet(sql, null);
        String userIds = recs.joinColumnValues("user", ",");

        return StringUtils2.joinIgnoreBlank(",", groupIds, userIds);
    }


    public String findGroupIdsByProperty(String propKey, String propVal, int max) {
        SQLBuilder.Select sql = new SQLBuilder.Select()
                .select("group_id")
                .from(groupPropertyTable)
                .where("key_=${v(key)} AND value_=${v(val)}", "key", propKey, "val", propVal);
        if (max > 0)
            sql.limit(max);


        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql.toString(), null);
        return recs.joinColumnValues("group_id", ",");
    }
}
