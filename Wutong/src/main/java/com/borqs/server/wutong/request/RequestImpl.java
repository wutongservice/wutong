package com.borqs.server.wutong.request;


import com.borqs.server.ServerException;
import com.borqs.server.base.conf.Configuration;
import com.borqs.server.base.conf.GlobalConfig;
import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.RecordSet;
import com.borqs.server.base.data.Schema;
import com.borqs.server.base.data.Schemas;
import com.borqs.server.base.log.Logger;
import com.borqs.server.base.log.TraceCall;
import com.borqs.server.base.sql.ConnectionFactory;
import com.borqs.server.base.sql.SQLExecutor;
import com.borqs.server.base.sql.SQLTemplate;
import com.borqs.server.base.sql.SQLUtils;
import com.borqs.server.base.util.*;
import com.borqs.server.base.util.json.JsonUtils;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.WutongErrors;
import com.borqs.server.wutong.commons.Commons;
import com.borqs.server.wutong.friendship.FriendshipLogic;
import com.borqs.server.wutong.group.GroupLogic;
import com.borqs.server.wutong.usersugg.SuggestedUserLogic;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.Validate;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.plexus.util.StringUtils;

import java.util.*;

import static com.borqs.server.wutong.Constants.*;

public class RequestImpl implements RequestLogic, Initializable {
    private static final Logger L = Logger.getLogger(RequestImpl.class);

    protected final Schema requestSchema = Schema.loadClassPath(RequestImpl.class, "request.schema");
    private ConnectionFactory connectionFactory;
    private String db;
    private String requestTable = "request";

    public RequestImpl() {
    }

    @Override
    public void init() {
        Configuration conf = GlobalConfig.get();
        requestSchema.loadAliases(conf.getString("schema.request.alias", null));
        this.connectionFactory = ConnectionFactory.getConnectionFactory(conf.getString("like.simple.connectionFactory", "dbcp"));
        this.db = conf.getString("like.simple.db", null);
        this.requestTable = conf.getString("like.simple.requestTable", "request");
    }

    @Override
    public void destroy() {
        this.requestTable = null;
        this.connectionFactory = ConnectionFactory.close(connectionFactory);
        db = null;
    }

    private static String toStr(Object o) {
        return ObjectUtils.toString(o);
    }

    private SQLExecutor getSqlExecutor() {
        return new SQLExecutor(connectionFactory, db);
    }

    protected long saveRequest(String userId, String sourceId, String app, String type, String message, String data, String options) {
        final String SQL1 = "SELECT ${alias.request_id} FROM ${table} WHERE ${alias.user}=${v(user)} AND ${alias.source}=${v(source)} AND ${alias.type}=${v(type)} AND ${alias.status}=0";
        final String SQL2 = "INSERT INTO ${table} ${values_join(alias, rec)}";
        final String SQL3 = "UPDATE ${table} SET ${alias.created_time}=${v(created_time)}, ${alias.message}=${v(message)}, ${alias.data}=${v(data)}, ${alias.options}=${v(options)} WHERE ${alias.user}=${v(user)} AND ${alias.source}=${v(source)} AND ${alias.type}=${v(type)} AND ${alias.status}=0";

        String sql = SQLTemplate.merge(SQL1, new Object[][]{
                {"table", requestTable},
                {"alias", requestSchema.getAllAliases()},
                {"user", Long.parseLong(userId)},
                {"source", Long.parseLong(sourceId)},
                {"type", type}});

        SQLExecutor se = getSqlExecutor();
        long requestId = se.executeIntScalar(sql, 0L);
        if (requestId != 0) {
            sql = SQLTemplate.merge(SQL3, new Object[][]{
                    {"table", requestTable},
                    {"alias", requestSchema.getAllAliases()},
                    {"created_time", DateUtils.nowMillis()},
                    {"message", org.apache.commons.lang.StringUtils.trimToEmpty(message)},
                    {"data", org.apache.commons.lang.StringUtils.trimToEmpty(data)},
                    {"options", org.apache.commons.lang.StringUtils.trimToEmpty(options)},
                    {"user", userId},
                    {"source", sourceId},
                    {"type", type},
            });
            se.executeUpdate(sql);
            return requestId;
        } else {
            requestId = RandomUtils.generateId();

            Record rec = new Record();
            rec.put("request_id", requestId);
            rec.put("user", Long.parseLong(userId));
            rec.put("source", Long.parseLong(sourceId));
            rec.put("app", Integer.parseInt(app));
            rec.put("type", org.apache.commons.lang.StringUtils.trimToEmpty(type));
            rec.put("created_time", DateUtils.nowMillis());
            rec.put("done_time", 0L);
            rec.put("status", 0);
            rec.put("message", org.apache.commons.lang.StringUtils.trimToEmpty(message));
            rec.put("data", org.apache.commons.lang.StringUtils.trimToEmpty(data));
            rec.put("options", org.apache.commons.lang.StringUtils.trimToEmpty(options));
            sql = SQLTemplate.merge(SQL2, "table", requestTable, "alias", requestSchema.getAllAliases(), "rec", rec);

            se.executeUpdate(sql);
            return requestId;
        }
    }

    @TraceCall
    @Override
    public String createRequest(Context ctx, String userId, String sourceId, String app, String type, String message, String data, String options) {
        try {
            Validate.isTrue(!StringUtils.equals(toStr(userId), toStr(sourceId)));
            long reqId = saveRequest(toStr(userId), toStr(sourceId), toStr(app), toStr(type), toStr(message), toStr(data), toStr(options));
            return Long.toString(reqId);
        } catch (Throwable t) {
            throw ErrorUtils.wrapResponseError(t);
        }
    }


    protected boolean saveRequests(String userIds, String sourceId, String app, String type, String message, String data, String options) {
        String[] users = StringUtils2.splitArray(userIds, ",", true);
        int size = users.length;
        if (size == 1)
            return saveRequest(userIds, sourceId, app, type, message, data, options) != 0;
        else {
            long[] requestIds = new long[size];
            for (int i = 0; i < size; i++)
                requestIds[i] = RandomUtils.generateId();

            String sql = "INSERT INTO " + requestTable + " (`request_id`, `user`, `source`, `app`, `type`, `created_time`, `done_time`, `status`, `message`, `data`, `options`) VALUES ";
            for (int i = 0; i < size; i++) {
                sql += "(" + requestIds[i] + ", " + users[i] + ", " + sourceId + ", " + app + ", '" + type + "', "
                        + DateUtils.nowMillis() + ", " + 0L + ", " + 0 + ", '" + message + "', '" + data + "', '" + options + "'), ";
            }
            sql = org.apache.commons.lang.StringUtils.substringBeforeLast(sql, ",");

            SQLExecutor se = getSqlExecutor();
            long n = se.executeUpdate(sql);
            return n > 0;
        }
    }

    @TraceCall
    @Override
    public boolean createRequests(Context ctx, String userIds, String sourceId, String app, String type, String message, String data, String options) {
        try {
            return saveRequests(toStr(userIds), toStr(sourceId), toStr(app), toStr(type), toStr(message), toStr(data), toStr(options));
        } catch (Throwable t) {
            throw ErrorUtils.wrapResponseError(t);
        }
    }

    protected void destroyRequests0(String userId, List<Long> reqIds) {
        final String SQL = "DELETE FROM ${table} WHERE request_id IN ${vjoin(reqids)}";
        if (reqIds.isEmpty())
            return;

        String sql = SQLTemplate.merge(SQL, "table", requestTable, "reqids", reqIds);
        SQLExecutor se = getSqlExecutor();
        se.executeUpdate(sql);
    }

    @TraceCall
    @Override
    public boolean destroyRequests(Context ctx, String userId, String requests) {
        try {
            List<Long> reqIds = StringUtils2.splitIntList(toStr(requests), ",");
            destroyRequests0(toStr(userId), reqIds);
            return true;
        } catch (Throwable t) {
            throw ErrorUtils.wrapResponseError(t);
        }
    }

    protected RecordSet getRequests0(String userId, String appId, String type) {
        final String SQL = "SELECT ${alias.request_id}, ${alias.source}, ${alias.app}, ${alias.type}, ${alias.created_time}, ${alias.message}, ${alias.data}" +
                " FROM ${table} WHERE ${alias.user}=${v(user)} AND ${alias.status}<>1";

        String sql = SQLTemplate.merge(SQL, "table", requestTable, "alias", requestSchema.getAllAliases(), "user", Long.parseLong(userId));
        if (org.apache.commons.lang.StringUtils.isNotBlank(appId))
            sql += " AND app=" + appId;
        if (org.apache.commons.lang.StringUtils.isNotBlank(type)) {
//            sql += " AND type=" + SQLUtils.toSql(type);
            String[] types = StringUtils2.splitArray(type, ",", true);
            sql += " AND type IN (" + SQLUtils.valueJoin(",", types) + ")";
        }
        sql += " ORDER BY created_time DESC";

        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        Schemas.standardize(requestSchema, recs);
        return recs;
    }

    @TraceCall
    @Override
    public RecordSet getRequests(Context ctx, String userId, String app, String type) {
        try {
            return getRequests0(toStr(userId), toStr(app), toStr(type));
        } catch (Throwable t) {
            throw ErrorUtils.wrapResponseError(t);
        }
    }

    protected boolean doneRequest0(String userId, List<Long> requestIds) {
        if (requestIds.isEmpty())
            return true;

        final String SQL = "UPDATE ${table} SET ${alias.status}=1, ${alias.done_time}=${v(now)} WHERE ${alias.request_id} IN (${vjoin(reqids)})";
        String sql = SQLTemplate.merge(SQL, "table", requestTable, "alias", requestSchema.getAllAliases(), "now", DateUtils.nowMillis(), "reqids", requestIds);
        SQLExecutor se = getSqlExecutor();
        return se.executeUpdate(sql) > 0;
    }

    @TraceCall
    @Override
    public boolean doneRequest(Context ctx, String userId, String requestIds) {
        try {
            return doneRequest0(toStr(userId), StringUtils2.splitIntList(toStr(requestIds), ","));
        } catch (Throwable t) {
            throw ErrorUtils.wrapResponseError(t);
        }
    }


    protected int getCount0(String userId, String app, String type) {
        final String SQL = "SELECT COUNT(*) FROM ${table} WHERE ${alias.user}=${v(user)} AND ${alias.status}<>1";

        String sql = SQLTemplate.merge(SQL, "table", requestTable, "alias", requestSchema.getAllAliases(), "user", Long.parseLong(userId));
        if (org.apache.commons.lang.StringUtils.isNotBlank(app))
            sql += " AND app=" + app;
        if (org.apache.commons.lang.StringUtils.isNotBlank(type))
            sql += " AND type=" + SQLUtils.toSql(type);

        SQLExecutor se = getSqlExecutor();
        return (int) se.executeIntScalar(sql, 0L);
    }

    @TraceCall
    @Override
    public int getCount(Context ctx, String userId, String app, String type) {
        try {
            return getCount0(toStr(userId), toStr(app), toStr(type));
        } catch (Throwable t) {
            throw ErrorUtils.wrapResponseError(t);
        }
    }


    protected String getPendingRequests0(String source, String user) {
        String types = "";
        final String SQL = "SELECT DISTINCT ${alias.type} FROM ${table} WHERE ${alias.source}=${source} AND ${alias.user}=${user} AND ${alias.status}=0";

        String sql = SQLTemplate.merge(SQL, new Object[][]{
                {"table", requestTable},
                {"alias", requestSchema.getAllAliases()},
                {"source", source},
                {"user", user}
        });

        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        Schemas.standardize(requestSchema, recs);

        for (Record rec : recs) {
            types += rec.getString("type") + ",";
        }

        return org.apache.commons.lang.StringUtils.substringBeforeLast(types, ",");
    }

    @TraceCall
    @Override
    public String getPendingRequests(Context ctx, String source, String user) {
        try {
            return getPendingRequests0(toStr(source), toStr(user));
        } catch (Throwable t) {
            throw ErrorUtils.wrapResponseError(t);
        }
    }

    protected RecordSet getPendingRequests1(String source, String userIds) {
        final String sql = "SELECT DISTINCT(type),user FROM request WHERE source=" + source + " AND user in (" + userIds + ") AND status=0";
        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        RecordSet out0 = new RecordSet();
        List<String> userl = StringUtils2.splitList(toStr(userIds), ",", true);
        for (String u : userl) {
            if (recs.size() > 0) {
                String types = "";
                for (Record rec : recs) {
                    if (rec.getString("user").equals(u)) {
                        types += rec.getString("type") + ",";
                    }
                }
                out0.add(Record.of("user", u, "penddingRequest", types));
            }

        }
        return out0;
    }

    @TraceCall
    @Override
    public RecordSet getPendingRequestsAll(Context ctx, String userId, String userIds) {
        try {
            return getPendingRequests1(toStr(userId), toStr(userIds));
        } catch (Throwable t) {
            throw ErrorUtils.wrapResponseError(t);
        }
    }

    protected String getRelatedRequestIds0(String sourceIds, String datas) {
        String sql = "SELECT request_id FROM " + requestTable + " WHERE data REGEXP '" + datas + "'";

        if (org.apache.commons.lang.StringUtils.isNotBlank(sourceIds) && !org.apache.commons.lang.StringUtils.equals(sourceIds, "0"))
            sql += " AND source IN (" + sourceIds + ")";

        SQLExecutor se = getSqlExecutor();
        RecordSet recs = se.executeRecordSet(sql, null);
        List<Long> requestIds = recs.getIntColumnValues("request_id");

        return StringUtils2.joinIgnoreBlank(",", requestIds);
    }

    @TraceCall
    @Override
    public String getRelatedRequestIds(Context ctx, String sourceIds, String datas) {
        try {
            return getRelatedRequestIds0(toStr(sourceIds), toStr(datas));
        } catch (Throwable t) {
            throw ErrorUtils.wrapResponseError(t);
        }
    }

    @TraceCall
    @Override
    public boolean dealRelatedRequestsP(Context ctx, String userId, String sourceIds, String datas) {
            String requestIds = toStr(getRelatedRequestIds(ctx, sourceIds, datas));
            return doneRequest(ctx, userId, requestIds);

    }


    // Platform
    @TraceCall
    @Override
    public int getRequestCountP(Context ctx, String userId, String app, String type) {
        if (!GlobalLogics.getAccount().hasUser(ctx, Long.parseLong(userId)))
            throw new ServerException(WutongErrors.USER_NOT_EXISTS, "User '%s' is not exists", userId);

        return getCount(ctx, userId, app, type);
    }


    @TraceCall
    @Override
    public boolean createRequestAttentionP(Context ctx, String userId, String friendId) {
        Validate.notNull(userId);

        SuggestedUserLogic su = GlobalLogics.getSuggest();
        FriendshipLogic fs = GlobalLogics.getFriendship();

        RecordSet recs = fs.getRelation(ctx, userId, friendId, String.valueOf(FRIENDS_CIRCLE));

        if (recs.size() == 0) {
            GlobalLogics.getSuggest().createSuggestUserP(ctx, userId, String.valueOf(friendId), Integer.valueOf(REQUEST_ATTENTION), "");

            Record u = GlobalLogics.getAccount().getUser(ctx, userId, userId, "display_name");


            Commons.sendNotification(ctx, NTF_REQUEST_ATTENTION,
                    Commons.createArrayNodeFromStrings(),
                    Commons.createArrayNodeFromStrings(userId),
                    Commons.createArrayNodeFromStrings(userId, u.getString("display_name")),
                    Commons.createArrayNodeFromStrings(),
                    Commons.createArrayNodeFromStrings(),
                    Commons.createArrayNodeFromStrings(userId, u.getString("display_name")),
                    Commons.createArrayNodeFromStrings(userId, u.getString("display_name")),
                    Commons.createArrayNodeFromStrings(),
                    Commons.createArrayNodeFromStrings(),
                    Commons.createArrayNodeFromStrings(userId),
                    Commons.createArrayNodeFromStrings(friendId)
            );


        }
        return true;
    }

    @TraceCall
    @Override
    public RecordSet getRequestsP(Context ctx, String userId, String app, String type) {
        if (!GlobalLogics.getAccount().hasUser(ctx, Long.parseLong(userId)))
            throw new ServerException(WutongErrors.USER_NOT_EXISTS, "User '%s' is not exists", userId);

        RecordSet recs = getRequests(ctx, userId, app, type);
        List<Long> sourceIds = recs.getIntColumnValues("source");
        RecordSet sourceRecs = GlobalLogics.getAccount().getUsers(ctx, ctx.getViewerIdString(), org.apache.commons.lang.StringUtils.join(sourceIds, ","), "user_id, display_name, remark, image_url, small_image_url, large_image_url, in_circles, contact_info,perhaps_name");
        sourceRecs.renameColumn("user_id", "uid");
        Map<String, Record> sources = sourceRecs.toRecordMap("uid");

        RecordSet rtRecs = new RecordSet();
        ArrayList<String> rmRequests = new ArrayList<String>();
        for (Record rec : recs) {
            String requestId = rec.checkGetString("request_id");
            String sourceId = rec.checkGetString("source");
            Record sourceRec = sources.get(sourceId);
            rec.put("source", sourceRec == null || sourceRec.isEmpty() ? JsonNodeFactory.instance.objectNode() : sourceRec.toJsonNode());
            if (sourceRec != null && !sourceRec.isEmpty()) {
                rtRecs.add(rec);
            } else {
                rmRequests.add(requestId);
            }
        }

        destroyRequests(ctx, userId, StringUtils2.joinIgnoreBlank(",", rmRequests));

        return rtRecs;

    }

    private boolean isGroupInviteRequest(String type) {
        String[] types = new String[]{REQUEST_PUBLIC_CIRCLE_INVITE, REQUEST_ACTIVITY_INVITE, REQUEST_ORGANIZATION_INVITE,
                REQUEST_GENERAL_GROUP_INVITE, REQUEST_EVENT_INVITE};
        return ArrayUtils.contains(types, type);
    }

    private boolean isGroupJoinRequest(String type) {
        String[] types = new String[]{REQUEST_PUBLIC_CIRCLE_JOIN, REQUEST_ACTIVITY_JOIN, REQUEST_ORGANIZATION_JOIN,
                REQUEST_GENERAL_GROUP_JOIN, REQUEST_EVENT_JOIN};
        return ArrayUtils.contains(types, type);
    }

    @TraceCall
    @Override
    public boolean doneRequestsP(Context ctx, String userId, String requestIds, String type, String data, boolean accept) {
        L.trace(ctx, "[Method doneRequests] requestIds: " + requestIds);
        L.trace(ctx, "[Method doneRequests] type: " + type);
        L.trace(ctx, "[Method doneRequests] data: " + data);
        L.trace(ctx, "[Method doneRequests] accept: " + accept);

        if (!GlobalLogics.getAccount().hasUser(ctx, Long.parseLong(userId)))
            throw new ServerException(WutongErrors.USER_NOT_EXISTS, "User '%s' is not exists", userId);

        GroupLogic group = GlobalLogics.getGroup();
        if (isGroupInviteRequest(type)) {
            JsonNode jn = JsonUtils.parse(data);
            long groupId = jn.get("group_id").getLongValue();
            if (accept) {
                group.addMembers(ctx, groupId, Record.of(userId, ROLE_MEMBER), true);
                requestIds = toStr(getRelatedRequestIds(ctx, "0", data));
            } else {
                Record statusRec = new Record();
                statusRec.put("user_id", Long.parseLong(userId));
                statusRec.put("display_name", GlobalLogics.getAccount().getUser(ctx, userId, userId, "display_name").getString("display_name"));
                statusRec.put("identify", "");
                statusRec.put("source", 0);
                statusRec.put("status", STATUS_REJECTED);
                group.addOrUpdatePendings(ctx, groupId, RecordSet.of(statusRec));
            }
        } else if (isGroupJoinRequest(type)) {
            JsonNode jn = JsonUtils.parse(data);
            String joiner = jn.get("user_id").getTextValue();
            String joinerName = jn.get("user_name").getTextValue();
            long groupId = jn.get("group_id").getLongValue();
            if (accept) {
                group.addMembers(ctx, groupId, Record.of(joiner, ROLE_MEMBER), true);
                requestIds = toStr(getRelatedRequestIds(ctx, joiner, data));
            } else {
                Record statusRec = new Record();
                statusRec.put("user_id", Long.parseLong(joiner));
                statusRec.put("display_name", joinerName);
                statusRec.put("identify", "");
                statusRec.put("source", 0);
                statusRec.put("status", STATUS_REJECTED);
                group.addOrUpdatePendings(ctx, groupId, RecordSet.of(statusRec));
            }
        }
        L.trace(ctx, "[Method doneRequests] before doneRequest requestIds: " + requestIds);

        return doneRequest(ctx, userId, requestIds);
    }


    @TraceCall
    @Override
    public String createRequestP(Context ctx, String userId, String sourceId, String app, String type, String message, String data, boolean addAddressCircle) {
        if (!GlobalLogics.getAccount().hasUser(ctx, Long.parseLong(userId)))
            throw new ServerException(WutongErrors.USER_NOT_EXISTS, "User '%s' is not exists", userId);

        if (!GlobalLogics.getAccount().hasUser(ctx, Long.parseLong(sourceId)))
            throw new ServerException(WutongErrors.USER_NOT_EXISTS, "User '%s' is not exists", sourceId);

        FriendshipLogic fs = GlobalLogics.getFriendship();

        //set friends
        if (addAddressCircle) {
            fs.setFriendsP(ctx, sourceId, userId, String.valueOf(ADDRESS_BOOK_CIRCLE), FRIEND_REASON_MANUALSELECT, true);
        }
        String str = toStr(createRequest(ctx, userId, sourceId, app, type, message, data, "[]"));

        //notif
        int count = getRequestCountP(ctx, userId, "0", REQUEST_PROFILE_ACCESS);
        String srcName = GlobalLogics.getAccount().getUser(ctx, sourceId, sourceId, "display_name").getString("display_name");

        Set<String> excludeReqs = new HashSet<String>();
        excludeReqs.add(REQUEST_ADD_FRIEND);
        excludeReqs.add(REQUEST_FRIEND_FEEDBACK);
        excludeReqs.add(REQUEST_ATTENTION);
        excludeReqs.add(REQUEST_PUBLIC_CIRCLE_INVITE);
        excludeReqs.add(REQUEST_PUBLIC_CIRCLE_JOIN);
        excludeReqs.add(REQUEST_ACTIVITY_INVITE);
        excludeReqs.add(REQUEST_ACTIVITY_JOIN);
        excludeReqs.add(REQUEST_ORGANIZATION_INVITE);
        excludeReqs.add(REQUEST_ORGANIZATION_JOIN);
        excludeReqs.add(REQUEST_GENERAL_GROUP_INVITE);
        excludeReqs.add(REQUEST_GENERAL_GROUP_JOIN);
        excludeReqs.add(REQUEST_EVENT_INVITE);
        excludeReqs.add(REQUEST_EVENT_JOIN);

        if (!excludeReqs.contains(type)) {
            Commons.sendNotification(ctx, NTF_NEW_REQUEST,
                    Commons.createArrayNodeFromStrings(),
                    Commons.createArrayNodeFromStrings(sourceId),
                    Commons.createArrayNodeFromStrings(),
                    Commons.createArrayNodeFromStrings(),
                    Commons.createArrayNodeFromStrings(),
                    Commons.createArrayNodeFromStrings(userId),
                    Commons.createArrayNodeFromStrings(),
                    Commons.createArrayNodeFromStrings(type, srcName, String.valueOf(count)),
                    Commons.createArrayNodeFromStrings(type, srcName, String.valueOf(count)),
                    Commons.createArrayNodeFromStrings(type),
                    Commons.createArrayNodeFromStrings(userId)
            );
        }

        return str;

    }
}