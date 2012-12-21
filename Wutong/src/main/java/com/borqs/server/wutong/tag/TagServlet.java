package com.borqs.server.wutong.tag;


import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.RecordSet;
import com.borqs.server.base.util.DateUtils;
import com.borqs.server.base.web.QueryParams;
import com.borqs.server.base.web.webmethod.WebMethod;
import com.borqs.server.base.web.webmethod.WebMethodServlet;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.commons.WutongContext;

public class TagServlet extends WebMethodServlet {
    @WebMethod("tag/create")
        public Record createTag(QueryParams qp)  {
            Context ctx = WutongContext.getContext(qp, true);
            TagLogic tagLogic = GlobalLogics.getTag();
        
            String viewerId = ctx.getViewerIdString();
            String tag = qp.checkGetString("tag");
            String type = qp.checkGetString("type");
            String taget_id = qp.checkGetString("target_id");
            Record record = Record.of("user",viewerId,"tag",tag,"type",type,"target_id",taget_id,"created_time", DateUtils.nowMillis());
    
            return tagLogic.createTag(ctx,record);
        }
        @WebMethod("tag/destroyed")
        public boolean destroyedTag(QueryParams qp)  {
            Context ctx = WutongContext.getContext(qp, true);
            TagLogic tagLogic = GlobalLogics.getTag();
    
            String viewerId = ctx.getViewerIdString();
            String tag = qp.checkGetString("tag");
            String type = qp.checkGetString("type");
            String taget_id = qp.checkGetString("target_id");
            Record record = Record.of("user",viewerId,"tag",tag,"type",type,"target_id",taget_id,"created_time",DateUtils.nowMillis());
    
            return tagLogic.destroyedTag(ctx,record);
        }
    
        @WebMethod("tag/finduserbytag")
        public RecordSet findUserByTag(QueryParams qp)  {
            Context ctx = WutongContext.getContext(qp, true);
            TagLogic tagLogic = GlobalLogics.getTag();
    
            String viewerId = ctx.getViewerIdString();
            String tag = qp.checkGetString("tag");
            int count = (int)qp.getInt("count",20);
            int page = (int)qp.getInt("page",0);
    
            return tagLogic.findUserByTag(ctx,tag,page,count);
        }
        @WebMethod("tag/hasTag")
        public boolean hasTag(QueryParams qp)  {
            Context ctx = WutongContext.getContext(qp, true);
            TagLogic tagLogic = GlobalLogics.getTag();
    
            String viewerId = ctx.getViewerIdString();
            String tag = qp.checkGetString("tag");
    
            return tagLogic.hasTag(ctx,viewerId,tag);
        }
    
        @WebMethod("tag/hasTarget")
        public boolean hasTarget(QueryParams qp)  {
            Context ctx = WutongContext.getContext(qp, true);
            TagLogic tagLogic = GlobalLogics.getTag();
    
            String viewerId = ctx.getViewerIdString();
            String tag = qp.checkGetString("target");
            String type = qp.checkGetString("type");
    
            return tagLogic.hasTarget(ctx,viewerId,tag,type);
        }
    
        @WebMethod("tag/findtagbyuser")
        public RecordSet findTagByUser(QueryParams qp)  {
            Context ctx = WutongContext.getContext(qp, true);
            TagLogic tagLogic = GlobalLogics.getTag();
    
            String viewerId = ctx.getViewerIdString();
    
            int count = (int)qp.getInt("count",20);
            int page = (int)qp.getInt("page",0);
    
            return tagLogic.findTagByUser(ctx,viewerId,page,count);
        }
    
        @WebMethod("tag/findtargetbyuser")
        public RecordSet findTargetByUser(QueryParams qp)  {
            Context ctx = WutongContext.getContext(qp, true);
            TagLogic tagLogic = GlobalLogics.getTag();
    
            String viewerId = ctx.getViewerIdString();
    
            String type = qp.checkGetString("type");
            int count = (int)qp.getInt("count",20);
            int page = (int)qp.getInt("page",0);
    
            return tagLogic.findTargetsByUser(ctx,viewerId,type,page,count);
        }
        @WebMethod("tag/findusertagbytarget")
        public RecordSet findUserTagByTarget(QueryParams qp)  {
            Context ctx = WutongContext.getContext(qp, true);
            TagLogic tagLogic = GlobalLogics.getTag();
    
            String viewerId = ctx.getViewerIdString();
    
            String target = qp.checkGetString("target");
            String type = qp.checkGetString("type");
            int count = (int)qp.getInt("count",20);
            int page = (int)qp.getInt("page",0);
    
            return tagLogic.findUserTagByTarget(ctx,target, type, page, count);
        }
}
