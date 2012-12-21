package com.borqs.server.wutong.notif;

import com.borqs.server.base.conf.Configuration;
import com.borqs.server.base.conf.GlobalConfig;
import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.log.Logger;
import com.borqs.server.wutong.Constants;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.setting.SettingImpl;
import org.apache.commons.lang.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class NotificationSender {
	protected String NOTIF_SERVER_ADDR = "192.168.5.208:8083";
	protected Notification notif = null;
	protected String receiverId = "";
	private static final Logger L = Logger.getLogger(NotificationSender.class);
	protected boolean isReplace = false;

	public NotificationSender()
	{
		Configuration conf = GlobalConfig.get();
		NOTIF_SERVER_ADDR = conf.getString("notif.server", "192.168.5.208:8083");
		notif = new Notification(NOTIF_SERVER_ADDR);
//		L.trace("Avro notif server address: " + NOTIF_SERVER_ADDR);
        GlobalLogics.init();
	}
	
	protected abstract List<Long> getScope(Context ctx, String senderId, Object... args);
	protected abstract String getSettingKey(Context ctx);
	
	protected String getAppId(Context ctx, Object... args)
	{
		return String.valueOf(findAppIdFromObjectType(ctx, (Integer)args[0]));
	}
	
	protected String getSenderId(Context ctx, Object... args)
	{
		return (args[0] == null) ? "" : (String)args[0];
	}
		
	protected String getTitle(Context ctx, Object... args)
	{
		return "";
	}
	
	protected String getAction(Context ctx, Object... args)
	{
		return "android.intent.action.VIEW";
	}
	
	protected String getType(Context ctx, Object... args)
	{
		return getSettingKey(ctx);
	}
	
	protected String getUri(Context ctx, Object... args)
	{
		return "";
	}
	
	protected String getTitleHtml(Context ctx, Object... args)
	{
		return getTitle(ctx, args);
	}
	
	protected String getBody(Context ctx, Object... args)
	{
		return "";
	}
	
	protected String getBodyHtml(Context ctx, Object... args)
	{
		return getBody(ctx, args);
	}

    protected String getData(Context ctx, Object... args)
	{
		return getUri(ctx, args);
	}
	
	protected String getObjectId(Context ctx, Object... args)
	{
		if((args != null) && (args.length > 0))
			return (String)args[0];
		else
		    return "";
	}
	
	protected Record createNotification(Context ctx, Object[][] args)
	{
		final String METHOD = "createNotification";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, args);
        
        Record msg = new Record();
		
        String appId = getAppId(ctx, args[0]);
        L.trace(ctx, "appId: " + appId);
        msg.put("appId", appId);
        
        String senderId = getSenderId(ctx, args[1]);
        L.trace(ctx, "senderId: " + senderId);
		msg.put("senderId", senderId);
        
        L.trace(ctx, "receiverId: " + receiverId);
		msg.put("receiverId", receiverId);
        
        String title = getTitle(ctx, args[2]);
        L.trace(ctx, "title: " + title);
		msg.put("title", title);
        
        String action = getAction(ctx, args[3]);
        L.trace(ctx, "action: " + action);
		msg.put("action", action);
        
        String type = getType(ctx, args[4]);
        L.trace(ctx, "type: " + type);
		msg.put("type", type);
        
        String uri = getUri(ctx, args[5]);
        L.trace(ctx, "uri: " + uri);
		msg.put("uri", uri);
		
        String data = getData(ctx, args[5]);
        L.trace(ctx, "data: " + data);
        msg.put("data", data);

		String titleHtml = getTitleHtml(ctx, args[6]);
        L.trace(ctx, "titleHtml: " + titleHtml);
        msg.put("titleHtml", titleHtml);
		
        String body = getBody(ctx, args[7]);
        L.trace(ctx, "body:" + body);
        msg.put("body", body);
        
        String bodyHtml = getBodyHtml(ctx, args[8]);
		L.trace(ctx, "bodyHtml: " + bodyHtml);
        msg.put("bodyHtml", bodyHtml);
        
        String objectId = getObjectId(ctx, args[9]);
        L.trace(ctx, "objectId: " + objectId);
		msg.put("objectId", objectId);
		
		if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
        return msg;
	}
	
	protected int findAppIdFromObjectType(Context ctx, int objectType) {
        int appId = Constants.APP_TYPE_BPC;
        
        if (objectType == Constants.USER_OBJECT)
            appId = Constants.APP_TYPE_BPC;
        
        else if (objectType == Constants.POST_OBJECT)
            appId = Constants.APP_TYPE_BPC;
        
        else if (objectType == Constants.VIDEO_OBJECT)
            appId = Constants.APP_TYPE_VIDEO;
        
        else if (objectType == Constants.APK_OBJECT)
            appId = Constants.APP_TYPE_QIUPU;
        
        else if (objectType == Constants.MUSIC_OBJECT)
            appId = Constants.APP_TYPE_MUSIC;
        
        else if (objectType == Constants.BOOK_OBJECT)
            appId = Constants.APP_TYPE_BROOK;
        
        return appId;
    }
	
	protected int findAppIdFromPostType(Context ctx, int postType) {
		int appId = Constants.APP_TYPE_BPC;
		
		if((postType == Constants.TEXT_POST) 
				|| (postType == Constants.LINK_POST))
			appId = Constants.APP_TYPE_BPC;
		
		else if(postType == Constants.VIDEO_POST)
			appId = Constants.APP_TYPE_BPC;
        else if(postType == Constants.PHOTO_POST)
			appId = Constants.APP_TYPE_BPC;
        else if(postType == Constants.AUDIO_POST)
			appId = Constants.APP_TYPE_BPC;
        else if(postType == Constants.FILE_POST)
			appId = Constants.APP_TYPE_BPC;
		
		else if((postType == Constants.BOOK_POST)
				|| (postType == Constants.BOOK_LIKE_POST)
				|| (postType == Constants.BOOK_COMMENT_POST))
			appId = Constants.APP_TYPE_BROOK;
			
		else if((postType == Constants.APK_POST)
				|| (postType == Constants.APK_LINK_POST)
				|| (postType == Constants.APK_COMMENT_POST)
				|| (postType == Constants.APK_LIKE_POST))
			appId = Constants.APP_TYPE_QIUPU;
		
		return appId;
	}
	
	public void send(Context ctx, Object[] scopeArgs, Object[][] args) {
        final String METHOD = "send";
        if (L.isTraceEnabled())
            L.traceStartCall(ctx, METHOD, scopeArgs, args);
        
        List<Long> l = getScope(ctx, getSenderId(ctx, args[1]), scopeArgs);
		L.trace(ctx, "send notification scope: " + l);
        if(l.size() > 0)
		{
            SettingImpl settingImpl = new SettingImpl();
            settingImpl.init();
            Record setting = settingImpl.getByUsers(ctx, getSettingKey(ctx), StringUtils.join(l, ","));

			Iterator iter = setting.entrySet().iterator();

			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry) iter.next();
				String userId = (String) entry.getKey();
				String value = (String) entry.getValue();

				if(value.equals("1"))
				{
					//refuse notification
					l.remove(Long.parseLong(userId));
				}
			}
			L.trace(ctx, "before scope join");
			receiverId = StringUtils.join(l, ",");
			L.debug(ctx, "send notification receiverId: " + receiverId);
            Record msg = createNotification(ctx, args);
            try{
            	L.debug(ctx, "send notification content: " + msg.toString(true, true));
                String result = notif.send(msg, isReplace);
                L.debug(ctx, "send notification result: " + result);
            }
            catch(Exception e){
                L.debug(ctx, "NOTIF_SERVER_ADDR ERROR!");
            }            
		}       
	
        if (L.isTraceEnabled())
            L.traceEndCall(ctx, METHOD);
    }
}