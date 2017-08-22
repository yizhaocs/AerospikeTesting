package com.adara.newcache.servlet;

import com.opinmind.ssc.cache.UserDataCache;
import com.opinmind.ssc.cache.UserDataCacheFactory;
import com.opinmind.ssc.cache.UserDataCacheImpl;
import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * scp /Users/yzhao/IdeaProjects/AerospikeTesting/dist/aerospiketesting.war manager:/home/yzhao
 * ssh manager
 * scp aerospiketesting.war qa-inweb1:/home/yzhao
 * ssh qa-inweb1
 * sudo cp aerospiketesting.war /opt/apache-tomcat/webapps/
 * sudo /sbin/service tomcat restart
 * curl "http://localhost:8080/aerospiketesting/memcacheTesting?mode=read&start=0&end=2000"
 * curl "http://localhost:8080/aerospiketesting/memcacheTesting?mode=read&start=0&end=2000"
 *
 *
 */

public class MemcacheTestingServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(MemcacheTestingServlet.class);
    private UserDataCache cache;

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String mode = req.getParameter("mode");
        int start =  Integer.valueOf(req.getParameter("start"));
        int end = Integer.valueOf(req.getParameter("end"));

        try {
            if (mode.equals("read")) {
                for (int i = start; i < end; i++) {
                    cache.getCookieIdMapping(i, String.valueOf(i + 1));
                }
            } else if (mode.equals("write")) {
                for (int i = start; i < end; i++) {
                    cache.setCookieIdMapping(i, String.valueOf(i + 1), Long.valueOf(i + 2));
                }
            }
        }catch(Exception e){
            System.out.println("[MemcacheTestingServlet.handleRequest]: ExceptionUtil.printExceptionInfo:");
            e.printStackTrace();
            log.error("[MemcacheTestingServlet.handleRequest]: [MemcacheTestingServlet.handleRequest]: ", e);
        }

    }


    public void init() throws ServletException {
        System.out.println("[MemcacheTestingServlet.init]");
    }

    public void destroy() throws ServletException {
        System.out.println("[MemcacheTestingServlet.destroy]");
    }


    public void setCache(UserDataCache cache) {
        this.cache = cache;
    }
}
