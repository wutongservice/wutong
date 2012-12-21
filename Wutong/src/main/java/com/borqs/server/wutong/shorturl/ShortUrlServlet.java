package com.borqs.server.wutong.shorturl;


import com.borqs.server.ServerException;
import com.borqs.server.base.conf.Configuration;
import com.borqs.server.base.web.QueryParams;
import com.borqs.server.base.web.webmethod.WebMethod;
import com.borqs.server.base.web.webmethod.WebMethodServlet;
import com.borqs.server.wutong.GlobalLogics;
import com.borqs.server.wutong.WutongErrors;
import org.apache.commons.lang.StringUtils;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class ShortUrlServlet extends WebMethodServlet {


    @WebMethod("link/longurl")
    public void getLongUrl(QueryParams qp, HttpServletResponse response) throws IOException {
        String param = qp.checkGetString("short_url");
        param = StringUtils.substringBefore(param, "\\");
        Configuration conf = getConfiguration();
        if (!param.toUpperCase().startsWith("HTTP://"))
            param = "http://" + param;
        String long_url = GlobalLogics.getShortUrl().getLongUrl(param);
        try {
            response.sendRedirect(long_url);
        } catch (IOException e) {
            throw new ServerException(WutongErrors.SYSTEM_HTTP_METHOD_NOT_SUPPORT, "url send redirect error");
        }
    }
}
