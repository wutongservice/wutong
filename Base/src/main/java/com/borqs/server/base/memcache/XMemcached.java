package com.borqs.server.base.memcache;

import com.borqs.server.base.conf.Configuration;
import com.borqs.server.base.conf.GlobalConfig;
import com.borqs.server.base.log.Logger;
import com.borqs.server.base.util.Initializable;
import com.borqs.server.base.util.StringUtils2;
import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClient;

import java.io.IOException;
public class XMemcached implements Initializable {
    private static final Logger L = Logger.getLogger(XMemcached.class);
    private MemcachedClient client;
    private int timeout =  60 * 24 * 30;

    @Override
    public void init() {
        Configuration conf = GlobalConfig.get();
//        Configuration conf = getConfig();
        try {
            client = new XMemcachedClient(conf.getString("platform.memcacheServerIpAddr", "localhost"), Integer.valueOf(conf.getString("platform.memcacheServerPortAddr", "11211")));
        } catch (IOException e) {
            L.debug(null, "memcached create error!:" + e.toString());
        }
    }

    @Override
    public void destroy() {
        try {
            client.shutdown();
        } catch (IOException e) {
            L.debug(null, "memcached shutdown error!:" + e.toString());
        }
    }

    public void writeCache(String key, String value) {
        try {
            client.set(key, timeout, StringUtils2.compress(value));
        } catch (Exception e) {
            L.debug(null, "write  memcached error!:" + e.toString());
        }
    }

    public String readCache(String key) {
        String value = "";
        try {
            Object getSomeObject = client.get(key);
            if (getSomeObject != null) {
                value = StringUtils2.uncompress(getSomeObject.toString());
            }
        } catch (Exception e) {
            L.debug(null, "read memcached error!:" + e.toString());
        }
        return value;
    }

    public void deleteCache(String key) {
        try {
            if (client.get(key)!=null)
                client.delete(key);
        } catch (Exception e) {
            L.debug(null, "delete memcached error!:" + e.toString());
        }
    }

    public void replaceCache(String key, String value) {
        try {
            client.replace(key, 0, StringUtils2.compress(value));
        } catch (Exception e) {
            L.debug(null, "replace memcached error!:" + e.toString());
        }
    }
}
