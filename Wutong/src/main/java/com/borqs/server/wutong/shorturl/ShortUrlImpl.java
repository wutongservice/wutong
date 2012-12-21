package com.borqs.server.wutong.shorturl;


import com.borqs.server.base.conf.Configuration;
import com.borqs.server.base.conf.GlobalConfig;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.sql.ConnectionFactory;
import com.borqs.server.base.sql.SQLExecutor;
import com.borqs.server.base.util.DateUtils;
import org.apache.commons.lang.StringUtils;

import java.net.MalformedURLException;
import java.net.URL;

import static com.borqs.server.base.util.ShortUrl.ShortText;

public class ShortUrlImpl implements ShortUrlLogic {
    private ConnectionFactory connectionFactory;
    private String db;
     private String SERVER_HOST = "api.borqs.com";

    public ShortUrlImpl() {
        Configuration conf = GlobalConfig.get();
        this.connectionFactory = ConnectionFactory.getConnectionFactory(conf.getString("account.simple.connectionFactory", "dbcp"));
        this.db = conf.getString("account.simple.db", null);
    }


    private SQLExecutor getSqlExecutor() {
        return new SQLExecutor(connectionFactory, db);
    }

    public String findLongUrl0(String short_url) {
        long dateTime = DateUtils.nowMillis();
        String sql = "select long_url from short_url where short_url='" + short_url + "'";
//        String sql = "select long_url from short_url where short_url='" + short_url + "' and failure_time>" + dateTime + "";
        SQLExecutor se = getSqlExecutor();
        Record rec = se.executeRecord(sql, null);
        String out_url = !rec.isEmpty() ? rec.getString("long_url") : "";
//        if (!rec.isEmpty()) {
//            if (!short_url.contains("borqs.com/z/v2mIRf")) {
//                String sql1 = "update short_url set failure_time = " + dateTime + " where short_url='" + short_url + "'";
//                se.executeUpdate(sql1);
//            }
//        }
        return out_url;
    }

    public boolean saveShortUrl0(String long_url, String short_url) {
        if (findLongUrl0(short_url).equals("")) {
            long dateTime = DateUtils.nowMillis();
            dateTime += 3 * 24 * 60 * 60 * 1000L;
            final String sql = "INSERT INTO short_url"
                    + " (long_url,short_url,failure_time)"
                    + " VALUES"
                    + " ('" + long_url + "','" + short_url + "','"+ dateTime +"')";
            SQLExecutor se = getSqlExecutor();
            se.executeUpdate(sql);
        }
        return true;
    }


    public String generalShortUrl(String long_url) {
            String short_url = "";
            if (!long_url.toUpperCase().startsWith("HTTP://"))
                long_url = "http://" + long_url;
            if (long_url.substring(long_url.length() - 1).equals("//") || long_url.substring(long_url.length() - 1).equals("\\")) {
                long_url = long_url.substring(0, long_url.length() - 1);
            }

            if (long_url.substring(long_url.lastIndexOf("\\") + 1, long_url.length()).contains("?")) {
                long_url += "&generate_time=" + DateUtils.nowMillis();
            } else {
                long_url += "?generate_time=" + DateUtils.nowMillis();
            }
            URL ur = null;
            try {
                ur = new URL(long_url);
            } catch (MalformedURLException e) {
            }
            String host = ur.getHost();    //api.borqs.com
            String lastUrlStr = StringUtils.replace(long_url, "http://" + host + "/", "");
            String formatUrl = ShortText(lastUrlStr)[0];
            short_url = "http://" + host + "/" + "z" + "/" + formatUrl;
            saveShortUrl0(long_url, short_url);
            return short_url;
    }

    public String getLongUrl(String short_url) {
        String out_url = findLongUrl0(short_url);
            if (out_url.length() < 10) {
                out_url = "http://" + SERVER_HOST + "/link/expired";
            }
            return out_url;
    }
}
