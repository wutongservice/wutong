package com.borqs.server.base.util.email;

import com.borqs.server.base.context.Context;
import com.borqs.server.base.log.Logger;
import org.codehaus.plexus.util.StringUtils;


public class AsyncSendMailFinal {
    private final static Logger log = Logger.getLogger(AsyncSendMailFinal.class);
    public static final String SMTP_BIZMAIL_YAHOO_COM = "smtp.bizmail.yahoo.com";
    public static final String SMTP_PORT = "465";
    private static AsyncSendMailFinal _instance;

    private String smtpFromUser;
    private String smtpFromUserPassword;

    private AsyncSendMailFinal(EmailModule email) {
        smtpFromUser = email.getSendEmailName();
        if(StringUtils.isBlank(smtpFromUser)){
            smtpFromUser = EmailModule.DEFAULT_SEND_EMAILNAME;
        }
        smtpFromUserPassword = email.getSendEmailPassword();
        if(StringUtils.isBlank(smtpFromUserPassword)){
            smtpFromUserPassword = EmailModule.DEFAULT_SEND_EMAILPASSWORD;
        }
    }

    synchronized public static AsyncSendMailFinal getInstance(EmailModule email) {
        if (_instance == null) {
            _instance = new AsyncSendMailFinal(email);
        }
        return _instance;
    }

    private static transient Dispatcher dispatcher;
    private boolean shutdown = false;

    public void asyncSendMailFinal(Context ctx, EmailModule email) {
        asyncSendMail(ctx, email);
    }


    public void asyncSendMail(final Context ctx, final EmailModule email) {
        log.debug(null, "entering asyncSendMessage");
        getDispatcher().invokeLater(new AsyncTask() {
            public void invoke() throws Exception {
                log.debug(null, "entering asyncSendMessage invoke method");
                SendMail sendMail = new SendMail();
                sendMail.setHTML(true);
                sendMail.setToField(email.getTo());
                sendMail.setSubjectField(email.getTitle());

                String mailContent = email.getContent();
                sendMail.setMessageText(mailContent);
                sendMail.setMyAddress(smtpFromUser);
                sendMail.sendMessage(email.getTitle(), SMTP_BIZMAIL_YAHOO_COM, SMTP_PORT, smtpFromUser, smtpFromUserPassword);

                log.debug(null, "Send email to: " + email.getTo());
            }
        });
    }


    private Dispatcher getDispatcher() {
        if (shutdown) {
            throw new IllegalStateException("Already shut down");
        }
        if (null == dispatcher) {
            dispatcher = new DispatcherFactory().getInstance();
        }
        return dispatcher;
    }

    abstract class AsyncTask implements Runnable {

        abstract void invoke() throws Exception;

        public void run() {
            try {
                invoke();
            } catch (Exception te) {

            }
        }
    }
}
