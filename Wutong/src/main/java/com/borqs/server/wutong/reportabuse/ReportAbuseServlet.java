package com.borqs.server.wutong.reportabuse;


import com.borqs.server.base.context.Context;
import com.borqs.server.base.web.QueryParams;
import com.borqs.server.base.web.webmethod.WebMethod;
import com.borqs.server.base.web.webmethod.WebMethodServlet;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.commons.Commons;
import com.borqs.server.wutong.commons.WutongContext;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;

public class ReportAbuseServlet extends WebMethodServlet {


    @WebMethod("post/report_abuse")
    public boolean reportAbuserCreate(QueryParams qp, HttpServletRequest req) throws UnsupportedEncodingException {
        Commons commons = new Commons();
        Context ctx = WutongContext.getContext(qp, true);
        String viewerId = ctx.getViewerIdString();
        String post_id = qp.checkGetString("post_id");
        String ua = commons.getDecodeHeader(req, "User-Agent", "", viewerId);
        String loc = commons.getDecodeHeader(req, "location", "", viewerId);
        return GlobalLogics.getReportAbuse().reportAbuserCreate(ctx, viewerId, post_id, ua, loc);
    }
}
