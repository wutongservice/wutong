package com.borqs.server.wutong.email;


import com.borqs.server.base.context.Context;
import com.borqs.server.base.util.email.EmailModule;

import java.util.Map;

public interface EmailLogic  {
    boolean sendCustomEmail(Context ctx,String title, String to, String username, String content, String type, String lang);
    boolean sendEmailHTML(Context ctx,String title, String to, String username, String content, String type, String lang);
   // boolean sendEmailEleaningHTML(Context ctx,String title, String to, String username, String content, String type, String lang);
    boolean sendInnovEmail(Context ctx,String title, String to,  Map<String,Object> map, String lang);

    
    //platform
    boolean sendCustomEmailP(Context ctx,String title, String to, String username, String templateFile, Map<String, Object> map, String type, String lang);

    boolean sendEmailFinal(Context ctx,EmailModule email);

    boolean sendEmail(Context ctx, String title, String to, String username, String content, String type, String lang);
}
