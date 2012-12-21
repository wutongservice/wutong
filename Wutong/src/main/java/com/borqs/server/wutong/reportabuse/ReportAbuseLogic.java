package com.borqs.server.wutong.reportabuse;


import com.borqs.server.base.context.Context;

public interface ReportAbuseLogic {

    int getReportAbuseCount(Context ctx,String post_id) ;

    int iHaveReport(Context ctx,String viewerId, String post_id) ;

    boolean reportAbuserCreate(Context ctx, String viewerId, String post_id, String ua, String loc) ;
}