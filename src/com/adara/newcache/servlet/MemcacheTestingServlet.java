package com.adara.newcache.servlet;

import com.opinmind.ssc.cache.UserDataCacheFactory;
import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * curl "http://localhost:8080/aero/memcacheTesting?mode=read"
 * curl "http://localhost:8080/aero/memcacheTesting?mode=write"
 *
 */

public class MemcacheTestingServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(MemcacheTestingServlet.class);
    private UserDataCacheFactory userDataCacheMC;

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String mode = req.getParameter("mode");
        if(mode.equals("read")){

        }else if(mode.equals("write")){

        }


    }


    public void init() throws ServletException {
        System.out.println("[MemcacheTestingServlet.init]");
    }

    public void destroy() throws ServletException {
        System.out.println("[MemcacheTestingServlet.destroy]");
    }


    public UserDataCacheFactory getUserDataCacheMC() {
        return userDataCacheMC;
    }

    public void setUserDataCacheMC(UserDataCacheFactory userDataCacheMC) {
        this.userDataCacheMC = userDataCacheMC;
    }
}
