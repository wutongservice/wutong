package com.borqs.server.wutong.page;


import com.borqs.server.ServerException;
import com.borqs.server.base.conf.Configuration;
import com.borqs.server.base.conf.GlobalConfig;
import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.RecordHandler;
import com.borqs.server.base.data.RecordSet;
import com.borqs.server.base.log.Logger;
import com.borqs.server.base.log.TraceCall;
import com.borqs.server.base.sql.ConnectionFactory;
import com.borqs.server.base.sql.SQLBuilder;
import com.borqs.server.base.sql.SQLExecutor;
import com.borqs.server.base.util.CollectionUtils2;
import com.borqs.server.base.util.DateUtils;
import com.borqs.server.base.util.ObjectHolder;
import com.borqs.server.base.util.StringUtils2;
import com.borqs.server.wutong.Constants;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.WutongErrors;
import com.borqs.server.wutong.commons.Commons;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

import java.util.LinkedHashSet;

public class PageImpl implements PageLogic {
    private static final Logger L = Logger.getLogger(PageImpl.class);

    private ConnectionFactory connectionFactory;
    private String db;
    private String pageTable;
    private String imagePattern;

    public PageImpl() {
        Configuration conf = GlobalConfig.get();
        this.connectionFactory = ConnectionFactory.getConnectionFactory(conf.getString("employeeList.simple.connectionFactory", "dbcp"));
        this.db = conf.getString("platform.simple.db", null);
        this.pageTable = conf.getString("page.simple.pageTable", "page");
        this.imagePattern = conf.getString("page.imageUrlPattern", "http://storage.aliyun.com/wutong-data/media/photo/%s");
    }

    private SQLExecutor getSqlExecutor() {
        return new SQLExecutor(connectionFactory, db);
    }

    /*
    @TraceCall
    @Override
    public Record createPageWithCircle(Context ctx, Record pageRec, String type, String subType) {
        boolean formal = "formal".equalsIgnoreCase(type);
        Record props = new Record();
        if (formal)
            props.set("type", ObjectUtils.toString(type)).set("subtype", ObjectUtils.toString(subType));

        GroupLogic group = GlobalLogics.getGroup();


        long groupId = group.createGroup(ctx, Constants.PUBLIC_CIRCLE_ID_BEGIN, Constants.TYPE_PUBLIC_CIRCLE,
                pageRec.getString("name"),  // name
                -1,                         // no limit
                formal ? 0 : 1, // streamPublic
                formal ? 0 : 1, // canSearch,
                formal ? 0 : 1, // canViewMembers,
                formal ? 0 : 1, // canJoin,
                formal ? 0 : 1, // canMemberInvite
                formal ? 0 : 1, // canMemberApprove,
                1, // canMemberPost,
                formal ? 0 : 1, // canMemberQuit,
                formal ? 0 : 1, // needInvitedConfirm,
                ctx.getViewerId(),
                "",
                props,
                !formal // sendPost
        );

        if (groupId <= 0)
            throw new ServerException(WutongErrors.GROUP_CREATE_ERROR, "Create group for page error");

        pageRec.set("associated_id", groupId);
        try {
            return createPage(ctx, pageRec);
        } catch (RuntimeException e) {
            group.destroyGroup(ctx, Long.toString(groupId));
            throw e;
        }
    }
    */

    @TraceCall
    @Override
    public Record createPage(Context ctx, Record pageRec) {
        long viewerId = ctx.getViewerId();
        GlobalLogics.getAccount().checkUserIds(ctx, Long.toString(viewerId));
        long associatedId = pageRec.checkGetInt("associated_id");
//        if (associatedId > 0) {
//            int userType = Constants.getUserTypeById(associatedId);
//            if (userType == Constants.PUBLIC_CIRCLE_OBJECT || userType == Constants.EVENT_OBJECT) {
//                // TODO: group exists?
//            } else if (userType == Constants.USER_OBJECT) {
//                // TODO: user exists?
//            } else {
//                throw new ServerException(WutongErrors.PAGE_ILLEGAL_ASSOCIATED_ID);
//            }
//        }

        long now = DateUtils.nowMillis();
        String sql = new SQLBuilder.Insert()
                .insertIgnoreInto(pageTable)
                .values(new Record()
                        .set("created_time", now)
                        .set("updated_time", now)
                        .set("destroyed_time", 0L)
                        .set("email_domain1", pageRec.getString("email_domain1"))
                        .set("email_domain2", pageRec.getString("email_domain2"))
                        .set("email_domain3", pageRec.getString("email_domain3"))
                        .set("email_domain4", pageRec.getString("email_domain4"))
                        .set("type", pageRec.getString("type"))
                        .set("name", pageRec.getString("name"))
                        .set("name_en", pageRec.getString("name_en"))
                        .set("email", pageRec.getString("email"))
                        .set("website", pageRec.getString("website"))
                        .set("tel", pageRec.getString("tel"))
                        .set("fax", pageRec.getString("fax"))
                        .set("zip_code", pageRec.getString("zip_code"))
                        .set("small_logo_url", pageRec.getString("small_logo_url"))
                        .set("logo_url", pageRec.getString("logo_url"))
                        .set("large_logo_url", pageRec.getString("large_logo_url"))
                        .set("small_cover_url", pageRec.getString("small_cover_url"))
                        .set("cover_url", pageRec.getString("cover_url"))
                        .set("large_cover_url", pageRec.getString("large_cover_url"))
                        .set("description", pageRec.getString("description"))
                        .set("description_en", pageRec.getString("description_en"))
                        .set("creator", viewerId)
                        .set("associated_id", associatedId)
                        .set("free_circle_ids", pageRec.getString("free_circle_ids"))
                ).toString();

        SQLExecutor se = getSqlExecutor();
        ObjectHolder<Long> idHolder = new ObjectHolder<Long>(0L);
        se.executeUpdate(sql, idHolder);
        if (idHolder.value == 0L)
            throw new ServerException(WutongErrors.PAGE_SERVICE_ERROR_CODE, "Create page error");

        return getPage(ctx, idHolder.value);
    }

    private void checkUpdatePagePermission(Context ctx, long pageId) {
        Record pageRec = getPage(ctx, pageId);
        if (pageRec.getInt("creator") != ctx.getViewerId())
            throw new ServerException(WutongErrors.PAGE_ILLEGAL_PERMISSION);
    }

    @TraceCall
    @Override
    public void destroyPage(Context ctx, long pageId, boolean destroyAssociated) {
        checkUpdatePagePermission(ctx, pageId);
        String sql;

        SQLExecutor se = getSqlExecutor();

        if (destroyAssociated) {
            sql = new SQLBuilder.Select()
                    .select("associated_id")
                    .from(pageTable)
                    .where("destroyed_time=0 AND page_id = ${v(page_id)}", "page_id", pageId)
                    .toString();
            long associatedId = se.executeIntScalar(sql, 0L);
            if (GlobalLogics.getGroup().isGroup(ctx, associatedId)) {
                GlobalLogics.getGroup().destroyGroup(ctx, Long.toString(associatedId));
            } else {
                GlobalLogics.getAccount().destroyAccount(ctx, Long.toString(associatedId));
            }
        }


        sql = new SQLBuilder.Update()
                .update(pageTable)
                .value("destroyed_time", DateUtils.nowMillis())
                .where("page_id=${v(page_id)}", "page_id", pageId)
                .toString();


        se.executeUpdate(sql);
    }

    @TraceCall
    @Override
    public void destroyPageByAssociated(Context ctx, long associatedId, boolean destroyAssociated) {
        long pageId = findPageIdByAssociated(ctx, associatedId);
        if (pageId > 0)
            destroyPage(ctx, pageId, destroyAssociated);
    }

    private long findPageIdByAssociated(Context ctx, long associatedId) {
        String sql = new SQLBuilder.Select()
                .select("page_id")
                .from(pageTable)
                .where("destroyed_time<>0 AND associated_id=${v(aid}}", "aid", associatedId)
                .toString();
        SQLExecutor se = getSqlExecutor();
        return se.executeIntScalar(sql, 0L);
    }

    @TraceCall
    @Override
    public Record updatePage(Context ctx, Record pageRec) {
        long pageId = pageRec.checkGetInt("page_id");
        checkUpdatePagePermission(ctx, pageId);
        long now = DateUtils.nowMillis();
        String sql = new SQLBuilder.Update()
                .update(pageTable)
                .value("updated_time", now)
                .valueIf(pageRec.containsKey("email_domain1"), "email_domain1", pageRec.getString("email_domain1"))
                .valueIf(pageRec.containsKey("email_domain2"), "email_domain2", pageRec.getString("email_domain2"))
                .valueIf(pageRec.containsKey("email_domain3"), "email_domain3", pageRec.getString("email_domain3"))
                .valueIf(pageRec.containsKey("email_domain4"), "email_domain4", pageRec.getString("email_domain4"))
                .valueIf(pageRec.containsKey("type"), "type", pageRec.getString("type"))
                .valueIf(pageRec.containsKey("name"), "name", pageRec.getString("name"))
                .valueIf(pageRec.containsKey("name_en"), "name_en", pageRec.getString("name_en"))
                .valueIf(pageRec.containsKey("address"), "address", pageRec.getString("address"))
                .valueIf(pageRec.containsKey("address_en"), "address_en", pageRec.getString("address_en"))
                .valueIf(pageRec.containsKey("email"), "email", pageRec.getString("email"))
                .valueIf(pageRec.containsKey("website"), "website", pageRec.getString("website"))
                .valueIf(pageRec.containsKey("tel"), "tel", pageRec.getString("tel"))
                .valueIf(pageRec.containsKey("fax"), "fax", pageRec.getString("fax"))
                .valueIf(pageRec.containsKey("zip_code"), "zip_code", pageRec.getString("zip_code"))
                .valueIf(pageRec.containsKey("small_logo_url"), "small_logo_url", pageRec.getString("small_logo_url"))
                .valueIf(pageRec.containsKey("logo_url"), "logo_url", pageRec.getString("logo_url"))
                .valueIf(pageRec.containsKey("large_logo_url"), "large_logo_url", pageRec.getString("large_logo_url"))
                .valueIf(pageRec.containsKey("small_cover_url"), "small_cover_url", pageRec.getString("small_cover_url"))
                .valueIf(pageRec.containsKey("cover_url"), "cover_url", pageRec.getString("cover_url"))
                .valueIf(pageRec.containsKey("large_cover_url"), "large_cover_url", pageRec.getString("large_cover_url"))
                .valueIf(pageRec.containsKey("description"), "description", pageRec.getString("description"))
                .valueIf(pageRec.containsKey("description_en"), "description_en", pageRec.getString("description_en"))
                .where("destroyed_time=0 AND page_id=${v(page_id)}", "page_id", pageId)
                .toString();

        SQLExecutor se = getSqlExecutor();
        long n = se.executeUpdate(sql);
        if (n == 0)
            throw new ServerException(WutongErrors.PAGE_ILLEGAL, "Illegal page " + pageId);

        return getPage(ctx, pageId);
    }

    private static void addImagePrefix(String imagePattern, Record rec) {
        if (rec.has("logo_url")) {
            if (!rec.getString("logo_url", "").startsWith("http:") && StringUtils.isNotBlank(rec.getString("logo_url")))
                rec.put("logo_url", String.format(imagePattern, rec.getString("logo_url")));
        }

        if (rec.has("small_logo_url")) {
            if (!rec.getString("small_logo_url", "").startsWith("http:")  && StringUtils.isNotBlank(rec.getString("small_logo_url")))
                rec.put("small_logo_url", String.format(imagePattern, rec.getString("small_logo_url")));
        }

        if (rec.has("large_logo_url")) {
            if (!rec.getString("large_logo_url", "").startsWith("http:") && StringUtils.isNotBlank(rec.getString("large_logo_url")))
                rec.put("large_logo_url", String.format(imagePattern, rec.getString("large_logo_url")));
        }

        if (rec.has("cover_url")) {
            if (!rec.getString("cover_url", "").startsWith("http:") && StringUtils.isNotBlank(rec.getString("cover_url")))
                rec.put("cover_url", String.format(imagePattern, rec.getString("cover_url")));
        }

        if (rec.has("small_cover_url")) {
            if (!rec.getString("small_cover_url", "").startsWith("http:") && StringUtils.isNotBlank(rec.getString("small_cover_url")))
                rec.put("small_cover_url", String.format(imagePattern, rec.getString("small_cover_url")));
        }

        if (rec.has("large_cover_url")) {
            if (!rec.getString("large_cover_url", "").startsWith("http:") && StringUtils.isNotBlank(rec.getString("large_cover_url")))
                rec.put("large_cover_url", String.format(imagePattern, rec.getString("large_cover_url")));
        }
    }

    @TraceCall
    @Override
    public RecordSet getPages(Context ctx, long[] pageIds) {
        RecordSet pageRecs = new RecordSet();
        if (ArrayUtils.isNotEmpty(pageIds)) {
            String sql = new SQLBuilder.Select()
                    .select(StringUtils2.splitArray(BASIC_COLS, ",", true))
                    .from(pageTable)
                    .where("destroyed_time=0 AND page_id IN (${page_ids})", "page_ids", StringUtils2.join(pageIds, ","))
                    .toString();
            SQLExecutor se = getSqlExecutor();
            se.executeRecordSet(sql, pageRecs);
            attachBasicInfo(ctx, pageRecs);
        }
        return pageRecs;
    }

    @TraceCall
    @Override
    public RecordSet getPagesForMe(Context ctx) {
        long viewerId = ctx.getViewerId();
        if (viewerId <= 0)
            return new RecordSet();

        final LinkedHashSet<Long> pageIds = new LinkedHashSet<Long>();

        String sql = new SQLBuilder.Select()
                .select("page_id")
                .from(pageTable)
                .where("destroyed_time=0 AND creator=${v(viewer_id)}", "viewer_id", viewerId)
                .toString();
        SQLExecutor se = getSqlExecutor();
        se.executeRecordHandler(sql, new RecordHandler() {
            @Override
            public void handle(Record rec) {
                pageIds.add(rec.getInt("page_id"));
            }
        });

        RecordSet groupRecs = GlobalLogics.getGroup().getGroups(ctx, Constants.GROUP_ID_BEGIN, Constants.GROUP_ID_END, ctx.getViewerIdString(), "", "page_id", false);
        for (Record groupRec : groupRecs) {
            long pageId = groupRec.getInt("page_id", 0L);
            if (pageId > 0)
                pageIds.add(pageId);
        }

        return getPages(ctx, CollectionUtils2.toLongArray(pageIds));
    }


    @TraceCall
    @Override
    public Record getPage(Context ctx, long pageId) {
        String sql = new SQLBuilder.Select()
                .select(StringUtils2.splitArray(BASIC_COLS, ",", true))
                .from(pageTable)
                .where("destroyed_time=0 AND page_id = ${v(page_id)}", "page_id", pageId)
                .toString();

        SQLExecutor se = getSqlExecutor();
        Record pageRec = se.executeRecord(sql, null);
        if (MapUtils.isEmpty(pageRec))
            throw new ServerException(WutongErrors.PAGE_ILLEGAL, "Illegal page " + pageId);

        attachBasicInfo(ctx, pageRec);
        attachDetailInfo(ctx, pageRec);
        return pageRec;
    }

    private void attachBasicInfo(Context ctx, Record pageRec) {
        // viewer_can_update
        if (ctx.getViewerId() >= 0 && pageRec.getInt("creator") == ctx.getViewerId()) {
            pageRec.put("viewer_can_update", true);
        } else {
            pageRec.put("viewer_can_update", false);
        }

        // logo & cover url prefix
        addImagePrefix(imagePattern, pageRec);

        // followers count
        int followersCount = GlobalLogics.getFriendship().getFollowersCount(ctx, pageRec.getString("page_id", "0"));
        pageRec.put("followers_count", followersCount);
    }

    private void attachBasicInfo(Context ctx, RecordSet pageRecs) {
        for (Record pageRec : pageRecs) {
            attachBasicInfo(ctx, pageRec);
        }
    }

    private void attachDetailInfo(Context ctx, Record pageRec) {
        Record rec = Commons.getUnifiedUser(ctx, pageRec.getInt("associated_id", 0L));
        String freeCircleIds = pageRec.getString("free_circle_ids");
        RecordSet freeCircleRecs;
        if (StringUtils.isBlank(freeCircleIds)) {
            freeCircleRecs = new RecordSet();
        } else {
            freeCircleRecs = GlobalLogics.getGroup().getGroups(ctx, Constants.PUBLIC_CIRCLE_ID_BEGIN, Constants.PUBLIC_CIRCLE_ID_END, ctx.getViewerIdString(), freeCircleIds, Constants.GROUP_LIGHT_COLS, false);
        }
        pageRec.put("associated", rec);
        pageRec.put("free_circles", freeCircleRecs);
    }

    @TraceCall
    @Override
    public RecordSet searchPages(Context ctx, String kw) {
        String skw = "%" + ObjectUtils.toString(kw) + "%";

        String sql = new SQLBuilder.Select()
                .select("page_id")
                .from(pageTable)
                .where("destroyed_time=0 AND (name LIKE ${v(kw)} OR name_en LIKE ${v(kw)}) OR address LIKE ${v(kw)} OR address_en LIKE ${v(kw)}", "kw", skw)
                .toString();

        SQLExecutor se = getSqlExecutor();
        RecordSet pageIdRecs = se.executeRecordSet(sql, null);
        long[] pageIds = CollectionUtils2.toLongArray(pageIdRecs.getIntColumnValues("page_id"));
        return getPages(ctx, pageIds);
    }
}
