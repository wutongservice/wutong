package com.borqs.server.wutong.tag;


import com.borqs.server.base.conf.Configuration;
import com.borqs.server.base.conf.GlobalConfig;
import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.RecordSet;
import com.borqs.server.base.data.Schema;
import com.borqs.server.base.log.Logger;
import com.borqs.server.base.sql.ConnectionFactory;
import com.borqs.server.base.sql.SQLExecutor;
import com.borqs.server.base.sql.SQLTemplate;
import com.borqs.server.base.sql.SQLUtils;
import com.borqs.server.base.util.Initializable;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class TagImpl implements TagLogic, Initializable {
    private static final Logger L = Logger.getLogger(TagImpl.class);
    public final Schema tagSchema = Schema.loadClassPath(TagImpl.class, "tag.schema");
    private ConnectionFactory connectionFactory;
    private String db;
    private String tag0 = "tag0";
    private String tag1 = "tag1";


    public void init() {
        Configuration conf = GlobalConfig.get();
        this.connectionFactory = ConnectionFactory.getConnectionFactory(conf.getString("account.simple.connectionFactory", "dbcp"));
        this.db = conf.getString("account.simple.db", null);
        this.tag0 = conf.getString("tag.simple.tag0", "tag0");
        this.tag1 = conf.getString("tag.simple.tag1", "tag1");
        tagSchema.loadAliases(GlobalConfig.get().getString("schema.tag.alias", null));
    }

    @Override
    public void destroy() {
        connectionFactory.close();
    }

    private SQLExecutor getSqlExecutor() {
        return new SQLExecutor(connectionFactory, db);
    }

    @Override
    public Record createTag(Context ctx, Record tag) {
        final String METHOD = "createTag";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, tag);
        final String SQL = "INSERT INTO ${table} ${values_join(alias, tag)}";

        String sql0 = SQLTemplate.merge(SQL,
                "table", this.tag0, "alias", tagSchema.getAllAliases(),
                "tag", tag);
        String sql1 = SQLTemplate.merge(SQL,
                "table", this.tag1, "alias", tagSchema.getAllAliases(),
                "tag", tag);
        List<String> sqls = new ArrayList<String>();
        sqls.add(sql0);
        sqls.add(sql1);
        SQLExecutor se = getSqlExecutor();
        L.op(ctx, "createTag");
        se.executeUpdate(sqls);
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return tag;
    }

    @Override
    public RecordSet findUserByTag(Context ctx, String tag, int page, int count) {
        final String METHOD = "findUserByTag";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, tag, page, count);
        String SQL = "SELECT * FROM ${table}"
                + " WHERE tag='" + tag + "'" + " and destroyed_time=0 ORDER BY created_time  ${limit}";
        String sql = SQLTemplate.merge(SQL, new Object[][]{
                {"table", this.tag1},
                {"limit", SQLUtils.pageToLimit(page, count)},
        });

        SQLExecutor se = getSqlExecutor();
        RecordSet rec = se.executeRecordSet(sql, null);
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return rec;
    }

    @Override
    public boolean hasTag(Context ctx, String user, String tag) {
        final String METHOD = "findUserByTag";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, tag);
        String SQL = "SELECT count(*) FROM ${table}"
                + " WHERE user='" + user + "'"
                + " and tag = '" + tag + "'";
        String sql = SQLTemplate.merge(SQL, new Object[][]{
                {"table", tag0},
        });

        SQLExecutor se = getSqlExecutor();
        long recs = se.executeIntScalar(sql, -1);
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return recs > 0;
    }

    @Override
    public boolean hasTarget(Context ctx, String user, String target_id, String type) {
        final String METHOD = "destroyedTag";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, user, target_id, type);
        String SQL = "SELECT count(*) FROM ${table}"
                + " WHERE user='" + user + "'"
                + " and target_id = '" + target_id + "'"
                + " and type =" + type;
        String sql = SQLTemplate.merge(SQL, new Object[][]{
                {"table", tag0},
        });

        SQLExecutor se = getSqlExecutor();
        long recs = se.executeIntScalar(sql, -1);
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return recs > 0;
    }

    @Override
    public RecordSet findTagByUser(Context ctx, String user, int page, int count) {
        final String METHOD = "destroyedTag";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, user, page, count);
        String SQL = "SELECT * FROM ${table}"
                + " WHERE user='" + user + "'" + " and destroyed_time=0 ORDER BY created_time  ${limit}";
        String sql = SQLTemplate.merge(SQL, new Object[][]{
                {"table", this.tag1},
                {"limit", SQLUtils.pageToLimit(page, count)},
        });

        SQLExecutor se = getSqlExecutor();
        RecordSet rec = se.executeRecordSet(sql, null);
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return rec;
    }

    @Override
    public RecordSet findTargetsByTag(Context ctx, String tag, String type, int page, int count) {
        final String METHOD = "destroyedTag";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, tag, type, page, count);
        String type0 = "";
        if (StringUtils.isNotEmpty(type))
            type0 = " and type = " + type;
        String SQL = "SELECT * FROM ${table}"
                + " WHERE tag='" + tag + "'"
                + type0
                + " and destroyed_time=0 ORDER BY created_time  ${limit}";
        String sql = SQLTemplate.merge(SQL, new Object[][]{
                {"table", this.tag1},
                {"limit", SQLUtils.pageToLimit(page, count)},
        });

        SQLExecutor se = getSqlExecutor();
        RecordSet rec = se.executeRecordSet(sql, null);
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return rec;
    }

    @Override
    public RecordSet findTargetsByUser(Context ctx, String user, String type, int page, int count) {
        final String METHOD = "destroyedTag";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, user, type, page, count);
        String type0 = "";
        if (StringUtils.isNotEmpty(type))
            type0 = " and type = " + type;
        String SQL = "SELECT * FROM ${table}"
                + " WHERE user='" + user + "'"
                + type0
                + " and destroyed_time=0 ORDER BY created_time  ${limit}";
        String sql = SQLTemplate.merge(SQL, new Object[][]{
                {"table", this.tag1},
                {"limit", SQLUtils.pageToLimit(page, count)},
        });

        SQLExecutor se = getSqlExecutor();
        RecordSet rec = se.executeRecordSet(sql, null);
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return rec;
    }

    @Override
    public RecordSet findUserTagByTarget(Context ctx, String target_id, String type, int page, int count) {
        final String METHOD = "destroyedTag";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, target_id, type, page, count);
        String SQL = "SELECT * FROM ${table}"
                + " WHERE target_id='" + target_id + "'"
                + " and type = " + type
                + " and destroyed_time=0 ORDER BY created_time  ${limit}";
        String sql = SQLTemplate.merge(SQL, new Object[][]{
                {"table", this.tag0},
                {"limit", SQLUtils.pageToLimit(page, count)},
        });

        SQLExecutor se = getSqlExecutor();
        RecordSet rec = se.executeRecordSet(sql, null);
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return rec;
    }

    /**
     * 删除tag，暂时不提供update方式的修改
     *
     * @param tag
     * @return
     */
    @Override
    public boolean destroyedTag(Context ctx, Record tag) {
        final String METHOD = "destroyedTag";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, tag);
        String sql0 = "delete from " + tag0
                + " where user = '" + tag.getString("user") + ";"
                + " and tag = " + tag.getString("tag") + "'"
                + " and target_id='" + tag.getString("target_id") + "'"
                + " and type='" + tag.getString("type") + "'";
        String sql1 = "delete from " + tag1
                + " where user = '" + tag.getString("user") + ";"
                + " and tag = " + tag.getString("tag") + "'"
                + " and target_id='" + tag.getString("target_id") + "'"
                + " and type='" + tag.getString("type") + "'";

        List<String> sqls = new ArrayList<String>();
        sqls.add(sql0);
        sqls.add(sql1);
        SQLExecutor se = getSqlExecutor();
        long n = se.executeUpdate(sqls);
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return n > 0;
    }
}
