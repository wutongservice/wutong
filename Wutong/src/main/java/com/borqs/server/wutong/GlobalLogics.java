package com.borqs.server.wutong;


import com.borqs.server.base.log.TraceCallInterceptor;
import com.borqs.server.base.util.ClassUtils2;
import com.borqs.server.base.util.InitUtils;
import com.borqs.server.base.web.JettyServer;
import com.borqs.server.wutong.account2.AccountImpl;
import com.borqs.server.wutong.account2.AccountLogic;
import com.borqs.server.wutong.app.AppLogic;
import com.borqs.server.wutong.comment.CommentImpl;
import com.borqs.server.wutong.comment.CommentLogic;
import com.borqs.server.wutong.commons.WutongHooks;
import com.borqs.server.wutong.company.CompanyImpl;
import com.borqs.server.wutong.company.CompanyLogic;
import com.borqs.server.wutong.conf.ConfImpl;
import com.borqs.server.wutong.conf.ConfLogic;
import com.borqs.server.wutong.contacts.SocialContactsImpl;
import com.borqs.server.wutong.contacts.SocialContactsLogic;
import com.borqs.server.wutong.conversation.ConversationImpl;
import com.borqs.server.wutong.conversation.ConversationLogic;
import com.borqs.server.wutong.email.EmailImpl;
import com.borqs.server.wutong.email.EmailLogic;
import com.borqs.server.wutong.folder.FolderImpl;
import com.borqs.server.wutong.folder.FolderLogic;
import com.borqs.server.wutong.friendship.FriendshipImpl;
import com.borqs.server.wutong.friendship.FriendshipLogic;
import com.borqs.server.wutong.group.EventThemeImpl;
import com.borqs.server.wutong.group.EventThemeLogic;
import com.borqs.server.wutong.group.GroupImpl;
import com.borqs.server.wutong.group.GroupLogic;
import com.borqs.server.wutong.ignore.IgnoreImpl;
import com.borqs.server.wutong.ignore.IgnoreLogic;
import com.borqs.server.wutong.like.LikeImpl;
import com.borqs.server.wutong.like.LikeLogic;
import com.borqs.server.wutong.nuser.setting.NUserSettingImpl;
import com.borqs.server.wutong.nuser.setting.NUserSettingLogic;
import com.borqs.server.wutong.page.PageImpl;
import com.borqs.server.wutong.page.PageLogic;
import com.borqs.server.wutong.photo.PhotoImpl;
import com.borqs.server.wutong.photo.PhotoLogic;
import com.borqs.server.wutong.poll.PollImpl;
import com.borqs.server.wutong.poll.PollLogic;
import com.borqs.server.wutong.reportabuse.ReportAbuseImpl;
import com.borqs.server.wutong.reportabuse.ReportAbuseLogic;
import com.borqs.server.wutong.request.RequestImpl;
import com.borqs.server.wutong.request.RequestLogic;
import com.borqs.server.wutong.setting.SettingImpl;
import com.borqs.server.wutong.setting.SettingLogic;
import com.borqs.server.wutong.shorturl.ShortUrlImpl;
import com.borqs.server.wutong.shorturl.ShortUrlLogic;
import com.borqs.server.wutong.signin.SignInImpl;
import com.borqs.server.wutong.signin.SignInLogic;
import com.borqs.server.wutong.statistics.StatisticsImpl;
import com.borqs.server.wutong.statistics.StatisticsLogic;
import com.borqs.server.wutong.stream.StreamImpl;
import com.borqs.server.wutong.stream.StreamLogic;
import com.borqs.server.wutong.tag.TagImpl;
import com.borqs.server.wutong.tag.TagLogic;
import com.borqs.server.wutong.usersugg.SuggestedUserImpl;
import com.borqs.server.wutong.usersugg.SuggestedUserLogic;
import com.borqs.server.wutong.verif.PhoneVerificationImpl;
import com.borqs.server.wutong.verif.PhoneVerificationLogic;

public class GlobalLogics {

    private static final WutongHooks hooks = new WutongHooks();

    private static AppLogic app = (AppLogic) ClassUtils2.newInstance(AppLogic.class, TraceCallInterceptor.INSTANCE);
    private static AccountLogic account = new AccountImpl();
    private static GroupLogic group = new GroupImpl();
    private static FriendshipLogic friendship = (FriendshipLogic) ClassUtils2.newInstance(FriendshipImpl.class, TraceCallInterceptor.INSTANCE);
    private static RequestLogic request = (RequestLogic) ClassUtils2.newInstance(RequestImpl.class, TraceCallInterceptor.INSTANCE);
    private static SuggestedUserLogic suggest = (SuggestedUserLogic) ClassUtils2.newInstance(SuggestedUserImpl.class, TraceCallInterceptor.INSTANCE);
    private static StreamLogic stream = new StreamImpl();
    private static PollLogic poll = new PollImpl();
    private static EmailLogic email = new EmailImpl();
    private static TagLogic tag = new TagImpl();
    private static SettingLogic setting = new SettingImpl();
    private static NUserSettingLogic newUsersetting = new NUserSettingImpl();
    private static ConfLogic conf = new ConfImpl();
    private static ReportAbuseLogic reportAbuse = new ReportAbuseImpl();
    private static IgnoreLogic ignore = new IgnoreImpl();
    private static ConversationLogic conversation = new ConversationImpl();
    private static SocialContactsLogic socialContacts = new SocialContactsImpl();
    private static PhotoLogic photo = new PhotoImpl();
    private static StatisticsLogic statisticsLogic = new StatisticsImpl();
    private static PhoneVerificationLogic phoneVerification = (PhoneVerificationLogic) ClassUtils2.newInstance(PhoneVerificationImpl.class, TraceCallInterceptor.INSTANCE);
    private static ShortUrlLogic shortUrlLogic= new ShortUrlImpl();
    private static FolderLogic folderLogic = new FolderImpl();
    private static LikeLogic likeLogic=new LikeImpl();
    private static CommentLogic commentLogic = new CommentImpl();
    private static EventThemeLogic eventTheme = (EventThemeLogic) ClassUtils2.newInstance(EventThemeImpl.class, TraceCallInterceptor.INSTANCE);
    private static SignInLogic signIn = (SignInLogic) ClassUtils2.newInstance(SignInImpl.class, TraceCallInterceptor.INSTANCE);
    private static CompanyLogic company = (CompanyLogic) ClassUtils2.newInstance(CompanyImpl.class, TraceCallInterceptor.INSTANCE);
    private static PageLogic pageLogic = (PageLogic) ClassUtils2.newInstance(PageImpl.class, TraceCallInterceptor.INSTANCE);


    public static void init() {
        InitUtils.batchInit(app, account,friendship,email,tag,setting,poll,reportAbuse,ignore,conversation,socialContacts,photo, phoneVerification,statisticsLogic, eventTheme, signIn, group,
        request,suggest,stream,folderLogic,likeLogic,commentLogic,shortUrlLogic, newUsersetting, conf, company, pageLogic);
    }

    public static WutongHooks getHooks() {
        return hooks;
    }

    public static void destroy() {
        InitUtils.batchInit(app, account,friendship,email,tag,setting,poll,reportAbuse,ignore,conversation,socialContacts,photo, phoneVerification,statisticsLogic, eventTheme, signIn, group,
                request,suggest,stream,folderLogic,likeLogic,commentLogic,shortUrlLogic, newUsersetting, conf, company, pageLogic);
    }

    public static AppLogic getApp() {
        return app;
    }

    public static AccountLogic getAccount() {
        return account;
    }


    public static GroupLogic getGroup() {
        return group;
    }

    public static FriendshipLogic getFriendship() {
        return friendship;
    }

    public static RequestLogic getRequest() {
        return request;
    }

    public static SuggestedUserLogic getSuggest() {
        return suggest;
    }

    public static StreamLogic getStream() {
        return stream;
    }

    public static PollLogic getPoll() {
        return poll;
    }

    public static SocialContactsLogic getSocialContacts() {
        return socialContacts;
    }

    public static EmailLogic getEmail() {
        return email;
    }

    public static TagLogic getTag() {
        return tag;
    }

    public static SettingLogic getSetting() {
        return setting;
    }

    public static NUserSettingLogic getNewUsersetting() {
        return newUsersetting;
    }

    public static ConfLogic getConf() {
        return conf;
    }

    public static ReportAbuseLogic getReportAbuse() {
        return reportAbuse;
    }

    public static IgnoreLogic getIgnore() {
        return ignore;
    }

    public static ConversationLogic getConversation() {
        return conversation;
    }

    public static PhotoLogic getPhoto() {
        return photo;
    }

    public static ShortUrlLogic getShortUrl() {
        return shortUrlLogic;
    }

    public static PhoneVerificationLogic getPhoneVerification() {
        return phoneVerification;
    }

    public static StatisticsLogic getStatisticsLogic() {
        return statisticsLogic;
    }

    public static FolderLogic getFile() {
        return folderLogic;
    }

    public static LikeLogic getLike() {
        return likeLogic;
    }

    public static CommentLogic getComment() {
        return commentLogic;
    }

    public static EventThemeLogic getEventTheme() {
        return eventTheme;
    }

    public static SignInLogic getSignIn() {
        return signIn;
    }

    public static CompanyLogic getCompany() {
        return company;
    }

    public static PageLogic getPage() {
        return pageLogic;
    }

    public static class ServerLifeCycle implements JettyServer.LifeCycle {
        @Override
        public void before() throws Exception {
            GlobalLogics.init();
        }

        @Override
        public void after() throws Exception {
            GlobalLogics.destroy();
        }
    }
}
