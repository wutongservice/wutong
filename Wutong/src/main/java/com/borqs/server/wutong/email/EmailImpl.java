package com.borqs.server.wutong.email;


import com.borqs.server.base.conf.Configuration;
import com.borqs.server.base.conf.GlobalConfig;
import com.borqs.server.base.context.Context;
import com.borqs.server.base.sql.SQLTemplate;
import com.borqs.server.base.util.I18nUtils;
import com.borqs.server.base.util.Initializable;
import com.borqs.server.base.util.email.AsyncSendMailUtil;
import com.borqs.server.base.util.email.EmailModule;
import com.borqs.server.base.web.template.PageTemplate;
import com.borqs.server.wutong.email.template.InnovTemplate;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class EmailImpl implements EmailLogic, Initializable {
    private static final PageTemplate pageTemplate = new PageTemplate(InnovTemplate.class);

    @Override
    public void init() {

    }
    @Override
    public void destroy() {
    }

    @Override
    public boolean sendEmail(Context ctx, String title, String to, String username, String content, String type, String lang) {
        Configuration conf = GlobalConfig.get();

        String serverHost = conf.getString("server.host", "api.borqs.com");
        String template;
        String subscribe = "";
        String shareTo = "";
        if (!StringUtils.equals(type, "email.essential") && !StringUtils.equals(type, "email.share_to")) {
            //            content = content + "		这封邮件是发送给<a href=mailto:" + to + ">" + to + "</a>的，<br>"
            //                + "     如果您不想再接收到此种类型的邮件，请点击<a href=http://" + serverHost + "/preferences/subscribe?user=" + to + "&type=" + type + "&value=1 target=_blank>退订</a>。<br>";
            template = I18nUtils.getBundleStringByLang(lang, "asyncsendmail.mailbottom.subscribe");
            subscribe = SQLTemplate.merge(template, new Object[][]{
                    {"to", to},
                    {"serverHost", serverHost},
                    {"type", type}
            });

        } else if (StringUtils.equals(type, "email.share_to")) {
            template = I18nUtils.getBundleStringByLang(lang, "asyncsendmail.mailbottom.shareto");
            shareTo = SQLTemplate.merge(template, new Object[][]{
                    {"to", to},
                    {"serverHost", serverHost},
                    {"type", type}
            });
        }
        Map<String, Object> map = new HashMap<String, Object>();


        map.put("host", serverHost);
        map.put("TitleName", I18nUtils.getBundleStringByLang(lang, "asyncsendmail.mailhead.borqsaccount"));
        map.put("subscribe", subscribe);
        map.put("shareTo", shareTo);
        map.put("content",content);
        String html = pageTemplate.merge("default.ftl", map);

        EmailModule email = new EmailModule(GlobalConfig.get());
        email.setContent(html);
        email.setTitle(title);
        email.setTo(to);
        email.setUsername(username);

        AsyncSendMailUtil.sendEmailFinal(ctx, email);
        return true;
    }

    @Override
    public boolean sendEmailHTML(Context ctx, String title, String to, String username, String content, String type, String lang) {
        EmailModule email = new EmailModule(GlobalConfig.get());
        email.setContent(content);
        email.setTitle(title);
        email.setTo(to);
        email.setUsername(username);

        AsyncSendMailUtil.sendEmailFinal(ctx, email);

        return true;
    }

    /*@Override
    public boolean sendEmailEleaningHTML(Context ctx, String title, String to, String username, String content, String type, String lang) {
        EmailModule email = new EmailModule(GlobalConfig.get());
        email.setContent(content);
        email.setTitle(title);
        email.setTo(to);
        email.setUsername(username);
        email.setSendEmailName(EmailModule.ELEARNING_SEND_EMAILNAME);
        email.setSendEmailPassword(EmailModule.ELEARNING_SEND_EMAILPASSWORD);
        AsyncSendMailUtil.sendEmailFinal(ctx, email);

        return true;
    }*/

    @Override
    public boolean sendInnovEmail(Context ctx, String title, String to, Map<String, Object> map, String lang) {
        String html = pageTemplate.merge("innov.ftl", map);
        EmailModule email = new EmailModule(GlobalConfig.get());
        email.setContent(html);
        email.setTitle(title);
        email.setTo(to);
        email.setSendEmailName(EmailModule.INNOV_SEND_EMAILNAME);
        email.setSendEmailPassword(EmailModule.INNOV_SEND_EMAILPASSWORD);
        AsyncSendMailUtil.sendEmailFinal(ctx, email);
        return true;

    }

    @Override
    public boolean sendCustomEmail(Context ctx, String title, String to,
                                   String username, String content, String type, String lang) {
        EmailModule email = new EmailModule(GlobalConfig.get());
        email.setContent(content);
        email.setTitle(title);
        email.setTo(to);
        email.setUsername(username);
        AsyncSendMailUtil.sendEmailFinal(ctx, email);

        return true;
    }

    @Override
    public boolean sendEmailFinal(Context ctx, EmailModule email) {
        AsyncSendMailUtil.sendEmailFinal(ctx, email);
        return true;
    }

    @Override
    public boolean sendCustomEmailP(Context ctx, String title, String to, String username, String templateFile, Map<String, Object> map, String type, String lang) {
        String html = pageTemplate.merge(templateFile, map);
        EmailModule email = new EmailModule(GlobalConfig.get());
        email.setContent(html);
        email.setTitle(title);
        email.setTo(to);
        email.setUsername(username);
        AsyncSendMailUtil.sendEmailFinal(ctx, email);
        return true;
    }
}
