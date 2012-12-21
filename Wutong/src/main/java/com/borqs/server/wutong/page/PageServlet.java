package com.borqs.server.wutong.page;


import com.borqs.server.ServerException;
import com.borqs.server.base.conf.Configuration;
import com.borqs.server.base.conf.GlobalConfig;
import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.RecordSet;
import com.borqs.server.base.sfs.SFSUtils;
import com.borqs.server.base.sfs.StaticFileStorage;
import com.borqs.server.base.sfs.oss.OssSFS;
import com.borqs.server.base.util.ClassUtils2;
import com.borqs.server.base.util.DateUtils;
import com.borqs.server.base.util.StringUtils2;
import com.borqs.server.base.web.QueryParams;
import com.borqs.server.base.web.webmethod.WebMethod;
import com.borqs.server.base.web.webmethod.WebMethodServlet;
import com.borqs.server.wutong.Constants;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.WutongErrors;
import com.borqs.server.wutong.commons.WutongContext;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.lang.StringUtils;

import javax.servlet.ServletException;

public class PageServlet extends WebMethodServlet {

    private StaticFileStorage photoStorage;

    public PageServlet() {
    }

    @Override
    public void init() throws ServletException {
        Configuration conf = GlobalConfig.get();
        photoStorage = (StaticFileStorage) ClassUtils2.newInstance(conf.getString("platform.servlet.photoStorage", ""));
        photoStorage.init();
    }

    @Override
    public void destroy() {
        photoStorage.destroy();
    }

    private static void readPage(Record pageRec, QueryParams qp) {
        pageRec.put("email_domain1", qp.getString("email_domain1", ""));
        pageRec.put("email_domain2", qp.getString("email_domain2", ""));
        pageRec.put("email_domain3", qp.getString("email_domain3", ""));
        pageRec.put("email_domain4", qp.getString("email_domain4", ""));
        pageRec.put("type", qp.getString("type", ""));
        pageRec.put("flags", qp.getInt("flags", 0L));
        pageRec.put("address", qp.getString("address", ""));
        pageRec.put("address_en", qp.getString("address_en", ""));
        pageRec.put("email", qp.getString("email", ""));
        pageRec.put("website", qp.getString("website", ""));
        pageRec.put("tel", qp.getString("tel", ""));
        pageRec.put("fax", qp.getString("fax", ""));
        pageRec.put("zip_code", qp.getString("zip_code", ""));
        pageRec.put("description", qp.getString("description", ""));
        pageRec.put("description_en", qp.getString("description_en", ""));
    }

    @WebMethod("page/create")
    public Record createPage(QueryParams qp) {
        Context ctx = WutongContext.getContext(qp, true);
        NamePair name = checkName(qp, false);
        Record pageRec = Record.of("name", name.name, "name_en", name.nameEn);
        readPage(pageRec, qp);
        pageRec.put("associated_id", 0L);
        pageRec.put("free_circle_ids", "");

        return GlobalLogics.getPage().createPage(ctx, pageRec);
    }

    @WebMethod("page/create_from")
    public Record createPageFrom(QueryParams qp) {
        Context ctx = WutongContext.getContext(qp, true);
        NamePair name = checkName(qp, false);
        long associatedId = qp.getInt("circle", 0);
        if (associatedId <= 0L)
            associatedId = ctx.getViewerId();

        Record pageRec = Record.of("name", name.name, "name_en", name.nameEn);
        readPage(pageRec, qp);

        long groupId = 0;
        if (Constants.getUserTypeById(associatedId) == Constants.PUBLIC_CIRCLE_OBJECT) {
            boolean isAdmin = GlobalLogics.getGroup().hasRight(ctx, associatedId, ctx.getViewerId(), Constants.ROLE_ADMIN);
            if (!isAdmin)
                throw new ServerException(WutongErrors.GROUP_RIGHT_ERROR, "");

            Record circleRec = GlobalLogics.getGroup().getGroup(ctx, ctx.getViewerIdString(), associatedId, Constants.GROUP_LIGHT_COLS + ", formal,page_id", false);
            if (circleRec != null)
                groupId = circleRec.getInt("id");

            if (circleRec != null && circleRec.getInt("page_id", 0L) > 0L)
                throw new ServerException(WutongErrors.PAGE_CIRCLE_ASSOCIATED, "The circle is associated with a page");

            if (circleRec != null && circleRec.getInt("formal") == 1) {
                pageRec.set("associated_id", associatedId);
                pageRec.set("free_circle_ids", "");
            } else {
                pageRec.set("associated_id", 0L);
                pageRec.set("free_circle_ids", Long.toString(associatedId));
            }
        } else {
            pageRec.set("associated_id", associatedId);
            pageRec.set("free_circle_ids", "");
        }

        Record resultPageRec = GlobalLogics.getPage().createPage(ctx, pageRec);
        if (groupId > 0) {
            GlobalLogics.getGroup().updateGroup(ctx, groupId, new Record(), Record.of("page_id", resultPageRec.getInt("page_id")));
        }
        return resultPageRec;
    }

    @WebMethod("page/destroy")
    public boolean destroyPage(QueryParams qp) {
        Context ctx = WutongContext.getContext(qp, true);
        long pageId = qp.checkGetInt("page");
        boolean withAssociated = qp.getBoolean("with_associated", false);
        GlobalLogics.getPage().destroyPage(ctx, pageId, withAssociated);
        return true;
    }

    @WebMethod("page/update")
    public Record updatePage(QueryParams qp) {
        Context ctx = WutongContext.getContext(qp, true);
        Record pageRec = new Record();
        pageRec.put("page_id", qp.checkGetInt("page_id"));
        pageRec.putIf("name", qp.getString("name", ""), qp.containsKey("name"));
        pageRec.putIf("name_en", qp.getString("name_en", ""), qp.containsKey("name_en"));
        pageRec.putIf("email_domain1", qp.getString("email_domain1", ""), qp.containsKey("email_domain1"));
        pageRec.putIf("email_domain2", qp.getString("email_domain2", ""), qp.containsKey("email_domain2"));
        pageRec.putIf("email_domain3", qp.getString("email_domain3", ""), qp.containsKey("email_domain3"));
        pageRec.putIf("email_domain4", qp.getString("email_domain4", ""), qp.containsKey("email_domain4"));
        pageRec.putIf("type", qp.getString("type", ""), qp.containsKey("type"));
        pageRec.putIf("flags", qp.getInt("flags", 0L), qp.containsKey("flags"));
        pageRec.putIf("address", qp.getString("address", ""), qp.containsKey("address"));
        pageRec.putIf("address_en", qp.getString("address_en", ""), qp.containsKey("address_en"));
        pageRec.putIf("email", qp.getString("email", ""), qp.containsKey("email"));
        pageRec.putIf("website", qp.getString("website", ""), qp.containsKey("website"));
        pageRec.putIf("tel", qp.getString("tel", ""), qp.containsKey("tel"));
        pageRec.putIf("fax", qp.getString("fax", ""), qp.containsKey("fax"));
        pageRec.putIf("zip_code", qp.getString("zip_code", ""), qp.containsKey("zip_code"));
        pageRec.putIf("description", qp.getString("description", ""), qp.containsKey("description"));
        pageRec.putIf("description_en", qp.getString("description_en", ""), qp.containsKey("description_en"));
        return GlobalLogics.getPage().updatePage(ctx, pageRec);
    }

    private String[] savePageImages(long pageId, String type, FileItem fi, boolean isLogo) {
        String[] urls = new String[3];

        long uploaded_time = DateUtils.nowMillis();
        String imageName = type + "_" + pageId + "_" + uploaded_time;

        String sfn = imageName + "_S.jpg";
        String ofn = imageName + "_M.jpg";
        String lfn = imageName + "_L.jpg";
        urls[0] = sfn;
        urls[1] = ofn;
        urls[2] = lfn;

        if (photoStorage instanceof OssSFS) {
            lfn = "media/photo/" + lfn;
            ofn = "media/photo/" + ofn;
            sfn = "media/photo/" + sfn;
        }

        if (isLogo) {
            SFSUtils.saveScaledUploadImage(fi, photoStorage, sfn, "50", "50", "jpg");
            SFSUtils.saveScaledUploadImage(fi, photoStorage, ofn, "80", "80", "jpg");
            SFSUtils.saveScaledUploadImage(fi, photoStorage, lfn, "180", "180", "jpg");
        } else {
            SFSUtils.saveUpload(fi, photoStorage, sfn);
            SFSUtils.saveUpload(fi, photoStorage, ofn);
            SFSUtils.saveUpload(fi, photoStorage, lfn);
        }

        return urls;
    }

    @WebMethod("page/upload_cover")
    public Record uploadCover(QueryParams qp) {
        Context ctx = WutongContext.getContext(qp, true);
        long pageId = qp.checkGetInt("page");
        FileItem fi = qp.checkGetFile("file");
        String[] urls = savePageImages(pageId, "p_cover", fi, false);
        Record pageRec = new Record();
        pageRec.put("page_id", pageId);
        pageRec.put("small_cover_url", urls[0]);
        pageRec.put("cover_url", urls[1]);
        pageRec.put("large_cover_url", urls[2]);
        return GlobalLogics.getPage().updatePage(ctx, pageRec);
    }

    @WebMethod("page/upload_logo")
    public Record uploadLogo(QueryParams qp) {
        Context ctx = WutongContext.getContext(qp, true);
        long pageId = qp.checkGetInt("page");
        FileItem fi = qp.checkGetFile("file");
        String[] urls = savePageImages(pageId, "p_logo", fi, true);
        Record pageRec = new Record();
        pageRec.put("page_id", pageId);
        pageRec.put("small_logo_url", urls[0]);
        pageRec.put("logo_url", urls[1]);
        pageRec.put("large_logo_url", urls[2]);
        return GlobalLogics.getPage().updatePage(ctx, pageRec);
    }

    @WebMethod("page/search")
    public RecordSet searchPages(QueryParams qp) {
        Context ctx = WutongContext.getContext(qp, false);
        String kw = StringUtils.trimToEmpty(qp.checkGetString("kw"));
        if (StringUtils.isEmpty(kw))
            throw new ServerException(WutongErrors.SYSTEM_MISS_REQUIRED_PARAMETER, "kw is blank");

        return GlobalLogics.getPage().searchPages(ctx, kw);
    }

    @WebMethod("page/show")
    public RecordSet showPages(QueryParams qp) {
        Context ctx = WutongContext.getContext(qp, false);
        String pageIds = qp.getString("pages", "");
        if (pageIds.isEmpty()) {
            return ctx.getViewerId() >= 0 ? GlobalLogics.getPage().getPagesForMe(ctx) : new RecordSet();
        } else {
            return GlobalLogics.getPage().getPages(ctx, StringUtils2.splitIntArray(pageIds, ","));
        }
    }

    @WebMethod("page/show1")
    public Record showPage(QueryParams qp) {
        Context ctx = WutongContext.getContext(qp, false);
        long pageId = qp.checkGetInt("page");
        return GlobalLogics.getPage().getPage(ctx, pageId);
    }

    private static NamePair checkName(QueryParams qp, boolean withPage) {
        String name = qp.getString(withPage ? "page_name" : "name", "");
        String nameEn = qp.getString(withPage ? "page_name_en" : "name_en", "");
        if (StringUtils.isEmpty(name) && StringUtils.isEmpty(nameEn))
            throw new ServerException(WutongErrors.SYSTEM_MISS_REQUIRED_PARAMETER, "Missing name or name_en");

        return new NamePair(name, nameEn);
    }

    private static class NamePair {
        String name;
        String nameEn;

        private NamePair(String name, String nameEn) {
            this.name = name;
            this.nameEn = nameEn;
        }
    }
}
