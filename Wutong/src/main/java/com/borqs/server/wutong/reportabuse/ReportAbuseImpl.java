package com.borqs.server.wutong.reportabuse;


import com.borqs.server.base.conf.Configuration;
import com.borqs.server.base.conf.GlobalConfig;
import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.Schema;
import com.borqs.server.base.log.Logger;
import com.borqs.server.base.sql.ConnectionFactory;
import com.borqs.server.base.sql.SQLExecutor;
import com.borqs.server.base.sql.SQLTemplate;
import com.borqs.server.base.util.DateUtils;
import com.borqs.server.base.util.Initializable;
import com.borqs.server.wutong.Constants;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.stream.StreamLogic;

public class ReportAbuseImpl implements ReportAbuseLogic, Initializable {
    private static final Logger L = Logger.getLogger(ReportAbuseImpl.class);
    private ConnectionFactory connectionFactory;
    private String db;
    private String reportAbuseTable = "report_abuse";
    public final Schema reportAbuseSchema = Schema.loadClassPath(ReportAbuseImpl.class, "reportabuse.schema");

    public void init() {
        Configuration conf = GlobalConfig.get();
        this.connectionFactory = ConnectionFactory.getConnectionFactory(conf.getString("account.simple.connectionFactory", "dbcp"));
        this.db = conf.getString("account.simple.db", null);
        this.reportAbuseTable = conf.getString("reportAbuse.simple.reportAbuseTable", "report_abuse");
    }

    @Override
    public void destroy() {
        connectionFactory.close();
    }

    private SQLExecutor getSqlExecutor() {
        return new SQLExecutor(connectionFactory, db);
    }

    public boolean saveReportAbuse(Context ctx, Record reportAbuse) {
        final String METHOD = "saveReportAbuse";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, reportAbuse);
        final String SQL = "INSERT INTO ${table} ${values_join(alias, reportAbuse)}";

        String sql = SQLTemplate.merge(SQL,
                "table", reportAbuseTable, "alias", reportAbuseSchema.getAllAliases(),
                "reportAbuse", reportAbuse);
        SQLExecutor se = getSqlExecutor();
        L.op(ctx, "saveReportAbuse");
        long n = se.executeUpdate(sql);
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return n > 0;
    }

    @Override
    public int getReportAbuseCount(Context ctx, String post_id) {
        final String METHOD = "saveReportAbuse";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, post_id);
        String sql = "select count(*) from " + reportAbuseTable + " where post_id in (" + post_id + ")";
        SQLExecutor se = getSqlExecutor();
        Number count = (Number) se.executeScalar(sql);
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return count.intValue();
    }

    @Override
    public int iHaveReport(Context ctx, String viewerId, String post_id) {
        final String METHOD = "iHaveReport";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, viewerId, post_id);
        String sql = "select count(*) from " + reportAbuseTable + " where post_id=" + post_id + " and user_id=" + viewerId + "";
        SQLExecutor se = getSqlExecutor();
        Number count = (Number) se.executeScalar(sql);
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return count.intValue();
    }

    @Override
    public boolean reportAbuserCreate(Context ctx, String viewerId, String post_id, String ua, String loc) {
        final String METHOD = "reportAbuserCreate";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, viewerId, post_id);
        Record rec = new Record();
        rec.put("post_id", post_id);
        rec.put("user_id", viewerId);
        rec.put("created_time", DateUtils.nowMillis());
        L.op(ctx, "reportAbuserCreate");
        boolean b = saveReportAbuse(ctx, rec);
        if (b) {
            String report = Constants.getBundleString(ua, "platform.sendmail.stream.report.abuse");
            StreamLogic streamLogic = GlobalLogics.getStream();
            Record this_stream = streamLogic.getPostP(ctx, post_id, "post_id,message");
            /*sendNotification(Constants.NTF_REPORT_ABUSE,
                    createArrayNodeFromStrings(),
                    createArrayNodeFromStrings(viewerId),
                    createArrayNodeFromStrings(post_id, viewerId, this_stream.getString("message")),
                    createArrayNodeFromStrings(),
                    createArrayNodeFromStrings(),
                    createArrayNodeFromStrings(post_id),
                    createArrayNodeFromStrings(post_id, viewerId, this_stream.getString("message")),
                    createArrayNodeFromStrings(report),
                    createArrayNodeFromStrings(report),
                    createArrayNodeFromStrings(post_id),
                    createArrayNodeFromStrings(post_id, viewerId)
            );*/
        }
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return b;

    }

}
