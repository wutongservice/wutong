package com.borqs.server.wutong.usersugg;


import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.RecordSet;
import com.borqs.server.base.web.QueryParams;
import com.borqs.server.base.web.webmethod.WebMethod;
import com.borqs.server.base.web.webmethod.WebMethodServlet;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.commons.WutongContext;
import org.apache.avro.AvroRemoteException;

public class SuggestedUserServlet extends WebMethodServlet {
    public SuggestedUserServlet() {
    }

    @WebMethod("suggest/refuse")
    public boolean suggestRefuse(QueryParams qp) throws AvroRemoteException {
        SuggestedUserLogic su = GlobalLogics.getSuggest();

        Context ctx = WutongContext.getContext(qp, true);
        return su.refuseSuggestUser(ctx, ctx.getViewerIdString(), qp.checkGetString("suggested"));
    }

    @WebMethod("suggest/create")
    public boolean suggestCreate(QueryParams qp) throws AvroRemoteException {
        SuggestedUserLogic su = GlobalLogics.getSuggest();

        Context ctx = WutongContext.getContext(qp, true);
        return su.createSuggestUserP(ctx, qp.getString("toUser",ctx.getViewerIdString()), qp.checkGetString("suggestedusers"), (int) qp.getInt("type", 90), qp.getString("reason", ""));
    }

    @WebMethod("suggest/delete")
    public boolean suggestDeleteSuggestUser(QueryParams qp) throws AvroRemoteException {
        SuggestedUserLogic su = GlobalLogics.getSuggest();

        Context ctx = WutongContext.getContext(qp, true);
        return su.deleteSuggestUserP(ctx, ctx.getViewerIdString(), qp.checkGetString("suggesteduser"));
    }

    @WebMethod("suggest/get")
    public RecordSet suggestGet(QueryParams qp) throws AvroRemoteException {
        SuggestedUserLogic su = GlobalLogics.getSuggest();

        Context ctx = WutongContext.getContext(qp, true);
        return su.getSuggestUserP(ctx, ctx.getViewerIdString(), (int) qp.getInt("count", 100),qp.getBoolean("getback",false));
    }
    @WebMethod("suggest/updatereason")
    public boolean suggestUpdateReason(QueryParams qp) throws AvroRemoteException {
        SuggestedUserLogic su = GlobalLogics.getSuggest();

        Context ctx = WutongContext.getContext(qp, false);
        return su.updateSuggestUserReasonP(ctx);
    }

    @WebMethod("suggest/recommend")
    public boolean suggestRecommend(QueryParams qp) throws AvroRemoteException {
        SuggestedUserLogic su = GlobalLogics.getSuggest();

        Context ctx = WutongContext.getContext(qp, true);
        try {
            return su.recommendUserP(ctx, ctx.getViewerIdString(), qp.checkGetString("touser"), qp.checkGetString("suggestedusers"));
        } catch (Exception e) {
            return false;
        }
    }
}
