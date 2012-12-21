package com.borqs.server.wutong.friendship;


import com.borqs.server.base.conf.GlobalConfig;
import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.RecordSet;
import com.borqs.server.base.util.StringUtils2;
import com.borqs.server.base.web.QueryParams;
import com.borqs.server.base.web.template.PageTemplate;
import com.borqs.server.base.web.webmethod.NoResponse;
import com.borqs.server.base.web.webmethod.WebMethod;
import com.borqs.server.base.web.webmethod.WebMethodServlet;
import com.borqs.server.wutong.Constants;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.account2.AccountLogic;
import com.borqs.server.wutong.commons.WutongContext;
import com.borqs.server.wutong.group.GroupLogic;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import static com.borqs.server.wutong.Constants.*;

public class FriendshipServlet extends WebMethodServlet {

    private static final PageTemplate pageTemplate = new PageTemplate(FriendshipServlet.class);


    public FriendshipServlet() {
    }

    @WebMethod("circle/create")
    public int createCircle(QueryParams qp) {
        FriendshipLogic fs = GlobalLogics.getFriendship();

        Context ctx = WutongContext.getContext(qp, true);
        return Integer.parseInt(fs.createCircle(ctx, ctx.getViewerIdString(), qp.checkGetString("name")));
    }

    @WebMethod("circle/destroy")
    public boolean destroyCircle(QueryParams qp) {
        FriendshipLogic fs = GlobalLogics.getFriendship();

        Context ctx = WutongContext.getContext(qp, true);
        return fs.destroyCircleP(ctx, ctx.getViewerIdString(), qp.checkGetString("circles"));
    }

    @WebMethod("circle/update")
    public boolean updateCircleName(QueryParams qp) {
        FriendshipLogic fs = GlobalLogics.getFriendship();

        Context ctx = WutongContext.getContext(qp, true);
        return fs.updateCircleName(ctx, ctx.getViewerIdString(), qp.checkGetString("circle"), qp.checkGetString("name"));
    }

    @WebMethod("circle/show")
    public RecordSet showCircles(QueryParams qp) {
        FriendshipLogic fs = GlobalLogics.getFriendship();
        GroupLogic group = GlobalLogics.getGroup();

        Context ctx = WutongContext.getContext(qp, false);
        String userId = qp.getString("user", ctx.getViewerIdString());


        boolean withPublicCircles = qp.getBoolean("with_public_circles", false);
        if (!withPublicCircles) {
            return fs.getCircles(ctx, userId, qp.getString("circles", ""), qp.getBoolean("with_users", false));
        } else {
            String circleIds = qp.getString("circles", "");
            boolean withMembers = qp.getBoolean("with_users", false);
            List<String> circles = StringUtils2.splitList(circleIds, ",", true);
            List<String> groups = group.getGroupIdsFromMentions(ctx, circles);
            circles.removeAll(groups);
            RecordSet recs = fs.getCirclesP(ctx, userId, StringUtils2.joinIgnoreBlank(",", circles), withMembers);
            RecordSet recs0 = group.getGroups(ctx,
                    PUBLIC_CIRCLE_ID_BEGIN, ACTIVITY_ID_BEGIN,
                    ctx.getViewerIdString(),
                    StringUtils2.joinIgnoreBlank(",", groups),
                    GROUP_LIGHT_COLS,
                    withMembers);
            recs0.renameColumn(GRP_COL_ID, "circle_id");
            recs0.renameColumn(GRP_COL_NAME, "circle_name");
            for (Record rec : recs)
                rec.put("type", CIRCLE_TYPE_LOCAL);
            for (Record rec : recs0)
                rec.put("type", CIRCLE_TYPE_PUBLIC);
            recs.addAll(recs0);
            return recs;
        }
    }

    @WebMethod("friend/usersset")
    public boolean setFriends(QueryParams qp, HttpServletRequest req) throws UnsupportedEncodingException {
        FriendshipLogic fs = GlobalLogics.getFriendship();

        Context ctx = WutongContext.getContext(qp, true);
        return fs.setFriendsP(ctx, ctx.getViewerIdString(), qp.checkGetString("friendIds"), qp.checkGetString("circleId"), Constants.FRIEND_REASON_MANUALSELECT, qp.getBoolean("isadd", true));
    }

    @WebMethod("friend/contactset")
    public Record setContactFriends(QueryParams qp, HttpServletRequest req) throws UnsupportedEncodingException {
        FriendshipLogic fs = GlobalLogics.getFriendship();
        AccountLogic account = GlobalLogics.getAccount();

        Context ctx = WutongContext.getContext(qp, true);

        Record rec = account.findUidLoginNameNotInID(ctx, qp.checkGetString("content"));
        String fid = "";
        String hasVirtualFriendId = fs.getUserFriendHasVirtualFriendId(ctx, ctx.getViewerIdString(), qp.checkGetString("content"));
        if (rec.isEmpty() && hasVirtualFriendId.equals("0")) {
            fid = fs.setContactFriendP(ctx, ctx.getViewerIdString(), qp.checkGetString("name"), qp.checkGetString("content"), qp.checkGetString("circleIds"), Constants.FRIEND_REASON_MANUALSELECT);
            return account.getUser(ctx, ctx.getViewerIdString(), fid, qp.getString("columns", AccountLogic.USER_LIGHT_COLUMNS));
        } else {
            fid = !rec.isEmpty() ? rec.getString("user_id") : hasVirtualFriendId;
            return fs.setFriendP(ctx, ctx.getViewerIdString(), fid, qp.checkGetString("circleIds"), Constants.FRIEND_REASON_MANUALSELECT);
        }
    }

    @WebMethod("friend/circlesset")
    public Record setCircles(QueryParams qp, HttpServletRequest req) throws UnsupportedEncodingException {
        FriendshipLogic fs = GlobalLogics.getFriendship();

        Context ctx = WutongContext.getContext(qp, true);
        return fs.setFriendP(ctx, ctx.getViewerIdString(), qp.checkGetString("friendId"), qp.checkGetString("circleIds"), Constants.FRIEND_REASON_MANUALSELECT);
    }

    @WebMethod("friend/exchange_vcard")
    public Record exchangeVcard(QueryParams qp, HttpServletRequest req) throws UnsupportedEncodingException {
        FriendshipLogic fs = GlobalLogics.getFriendship();
        Context ctx = WutongContext.getContext(qp, true);

        boolean send_request = qp.getBoolean("send_request",false);
        return fs.exchangeVcardP(ctx, ctx.getViewerIdString(), qp.checkGetString("friendId"), qp.checkGetString("circleIds"), Constants.FRIEND_REASON_MANUALSELECT, send_request);
    }

    @WebMethod("friend/mutual")
    public NoResponse mutualFriend(QueryParams qp, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        final String SERVER_HOST = GlobalConfig.get().getString("server.host", "api.borqs.com");;

        FriendshipLogic fs = GlobalLogics.getFriendship();

        Context ctx = WutongContext.getContext(qp, false);
        String userId = qp.checkGetString("user_id");
        String fromId = qp.checkGetString("from_id");
        try
        {
            fs.setFriendsP(ctx, userId, fromId, String.valueOf(Constants.ACQUAINTANCE_CIRCLE), Constants.FRIEND_REASON_INVITE, true);
            fs.setFriendsP(ctx, fromId, userId, String.valueOf(Constants.ACQUAINTANCE_CIRCLE), Constants.FRIEND_REASON_INVITE, true);
            fs.setFriendsP(ctx, userId, fromId, String.valueOf(Constants.ADDRESS_BOOK_CIRCLE), Constants.FRIEND_REASON_INVITE, true);
            fs.setFriendsP(ctx, fromId, userId, String.valueOf(Constants.ADDRESS_BOOK_CIRCLE), Constants.FRIEND_REASON_INVITE, true);
        }
        catch (Exception e)
        {
            String html = pageTemplate.merge("notice.ftl", new Object[][]{
                    {"host", SERVER_HOST},
                    {"notice", "互相加为好友失败，请稍后再试。"}
            });
            resp.setContentType("text/html");
            resp.getWriter().print(html);

            return NoResponse.get();
        }

        String html = pageTemplate.merge("notice.ftl", new Object[][]{
                {"host", SERVER_HOST},
                {"notice", "互相加为好友成功！"}
        });
        resp.setContentType("text/html");
        resp.getWriter().print(html);

        return NoResponse.get();
    }


    @WebMethod("friend/show")
    public RecordSet showFriends(QueryParams qp) {
        final int DEFAULT_USER_COUNT_IN_PAGE = 20;

        FriendshipLogic fs = GlobalLogics.getFriendship();

        Context ctx = WutongContext.getContext(qp, true);
        String userId = qp.getString("user", ctx.getViewerIdString());
        boolean withPublicCircles = qp.getBoolean("with_public_circles", false);
        if (!withPublicCircles)
            return fs.getFriendsP(ctx, ctx.getViewerIdString(), userId,
                    qp.getString("circles", Integer.toString(FRIENDS_CIRCLE)),
                    qp.getString("columns", AccountLogic.USER_STANDARD_COLUMNS),
                    qp.getBoolean("in_public_circles", false),
                    (int) qp.getInt("page", 0),
                    (int) qp.getInt("count", DEFAULT_USER_COUNT_IN_PAGE));
        else
            return fs.getFriendsV2P(ctx, ctx.getViewerIdString(), userId,
                    qp.getString("circles", Integer.toString(FRIENDS_CIRCLE)),
                    qp.getString("columns", AccountLogic.USER_STANDARD_COLUMNS),
                    (int) qp.getInt("page", 0),
                    (int) qp.getInt("count", DEFAULT_USER_COUNT_IN_PAGE));
    }


    @WebMethod("friend/both")
    public RecordSet getBothFriends(QueryParams qp) {
        final int DEFAULT_USER_COUNT_IN_PAGE = 20;

        FriendshipLogic fs = GlobalLogics.getFriendship();

        Context ctx = WutongContext.getContext(qp, true);
        String userId = qp.getString("user", ctx.getViewerIdString());
        return fs.getBothFriendsP(ctx, ctx.getViewerIdString(), userId,
                (int) qp.getInt("page", 0),
                (int) qp.getInt("count", DEFAULT_USER_COUNT_IN_PAGE));
    }

    @WebMethod("follower/show")
    public RecordSet showFollowers(QueryParams qp) {
        final int DEFAULT_USER_COUNT_IN_PAGE = 20;

        FriendshipLogic fs = GlobalLogics.getFriendship();

        Context ctx = WutongContext.getContext(qp, true);
        String userId = qp.getString("user", ctx.getViewerIdString());
        return fs.getFollowersP(ctx, ctx.getViewerIdString(), userId,
                qp.getString("circles", Integer.toString(FRIENDS_CIRCLE)),
                qp.getString("columns", AccountLogic.USER_STANDARD_COLUMNS),
                (int) qp.getInt("page", 0),
                (int) qp.getInt("count", DEFAULT_USER_COUNT_IN_PAGE));
    }


    @WebMethod("relation/get")
    public RecordSet getRelation(QueryParams qp) {
        FriendshipLogic fs = GlobalLogics.getFriendship();

        Context ctx = WutongContext.getContext(qp, true);
        String userId = qp.getString("source", ctx.getViewerIdString());
        return fs.getRelation(ctx, userId, qp.checkGetString("target"), qp.getString("circle", Integer.toString(FRIENDS_CIRCLE)));
    }


    @WebMethod("relation/bidi")
    public Record getBidiRelation(QueryParams qp) {
        FriendshipLogic fs = GlobalLogics.getFriendship();

        Context ctx = WutongContext.getContext(qp, true);
        String userId = qp.getString("source", ctx.getViewerIdString());
        return fs.getBidiRelation(ctx, userId, qp.checkGetString("target"), qp.getString("circle", Integer.toString(FRIENDS_CIRCLE)));
    }


    @WebMethod("remark/set")
    public boolean setUserRemark(QueryParams qp) {
        FriendshipLogic fs = GlobalLogics.getFriendship();

        Context ctx = WutongContext.getContext(qp, true);
        return fs.setRemark(ctx, ctx.getViewerIdString(), qp.checkGetString("friend"), qp.getString("remark", ""));
    }
}
