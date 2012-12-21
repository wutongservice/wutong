package com.borqs.server.wutong.commons;


import com.borqs.server.ServerException;
import com.borqs.server.base.auth.WebSignatures;
import com.borqs.server.base.context.Context;
import com.borqs.server.base.util.RandomUtils;
import com.borqs.server.base.web.QueryParams;
import com.borqs.server.wutong.Constants;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.WutongErrors;
import org.apache.commons.lang.StringUtils;

public class WutongContext {

    public static String checkTicket(QueryParams qp) {
        String userId = GlobalLogics.getAccount().whoLogined(Context.dummy(), qp.checkGetString("ticket"));
        if (Constants.isNullUserId(userId))
            throw new ServerException(WutongErrors.AUTH_TICKET_INVALID, "Invalid ticket");
        return userId;
    }

    public static void checkSign(QueryParams qp)  {
        Context ctx = Context.dummy();
        String appId = qp.checkGetString("appid");
        String sign = qp.checkGetString("sign");
        String signMethod = qp.getString("sign_method", "md5");

        String secret = GlobalLogics.getApp().getAppSecret(ctx, Integer.parseInt(appId));
        if (secret == null)
            throw new ServerException(WutongErrors.AUTH_TICKET_INVALID, "App secret error");

        if (!"md5".equalsIgnoreCase(signMethod))
            throw new ServerException(WutongErrors.AUTH_TICKET_INVALID, "Invalid sign method");

        String expectantSign = WebSignatures.md5Sign(secret, qp.keySet());
        if (!StringUtils.equals(sign, expectantSign))
            throw new ServerException(WutongErrors.AUTH_TICKET_INVALID, "Invalid md5 signatures");
    }

    public static String checkSignAndTicket(QueryParams qp) {
        checkSign(qp);
        return checkTicket(qp);
    }

    public static Context getContextWithoutAppAndViewer(QueryParams qp) {
        Context ctx = new Context();

        // client call id
        ctx.setClientCallId(qp.getString("call_id", ""));

        // server call id
        ctx.setServerCallId(Long.toString(RandomUtils.generateId()));

        // user agent
        ctx.setUa(qp.getString("$ua", ""));

        // location
        ctx.setLocation(qp.getString("$loc", ""));

        // language
        ctx.setLanguage(qp.getString("$lang", ""));

        // appId
        ctx.setAppId("0");

        // viewerId
        ctx.setViewerId(0L);

        return ctx;
    }

    public static Context getContext(QueryParams qp, boolean needLogin) {
        Context ctx = new Context();


        // client call id
        ctx.setClientCallId(qp.getString("call_id", ""));

        // server call id
        ctx.setServerCallId(Long.toString(RandomUtils.generateId()));

        // user agent
        ctx.setUa(qp.getString("$ua", ""));

        // location
        ctx.setLocation(qp.getString("$loc", ""));

        // language
        ctx.setLanguage(qp.getString("$lang", ""));

        // check sign
        String appId = qp.getString("appid", null);
        if (StringUtils.isNotEmpty(appId)) {
            String sign = qp.checkGetString("sign");
            String signMethod = qp.getString("sign_method", "md5");

            String secret = GlobalLogics.getApp().getAppSecret(ctx, Integer.parseInt(appId));
            if (secret == null)
                throw new ServerException(WutongErrors.AUTH_TICKET_INVALID, "App secret error");

            if (!"md5".equalsIgnoreCase(signMethod))
                throw new ServerException(WutongErrors.AUTH_TICKET_INVALID, "Invalid sign method");

            String expectantSign = WebSignatures.md5Sign(secret, qp.keySet());
            if (!StringUtils.equals(sign, expectantSign))
                throw new ServerException(WutongErrors.AUTH_TICKET_INVALID, "Invalid md5 signatures");

            ctx.setAppId(appId);
        } else {
            ctx.setAppId("0");
        }


        // check ticket
        String ticket = qp.getString("ticket", null);
        if (needLogin && StringUtils.isEmpty(ticket))
            throw new ServerException(WutongErrors.AUTH_TICKET_INVALID, "Need ticket");

        if (StringUtils.isNotEmpty(ticket)) {
            String viewerId = GlobalLogics.getAccount().whoLogined(ctx, ticket);
            if (Constants.isNullUserId(viewerId))
                throw new ServerException(WutongErrors.AUTH_TICKET_INVALID, "Invalid ticket");

            ctx.setViewerId(Long.parseLong(viewerId));
        } else {
            ctx.setViewerId(0L);
        }

        return ctx;
    }

}
